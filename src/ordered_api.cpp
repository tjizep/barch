//
// Created by teejip on 4/9/25.
//

#include "ordered_api.h"
#include "conversion.h"
#include "art.h"
#include "composite.h"
#include "keys.h"
#include "module.h"
// TODO: one day this counters gonna wrap
static std::atomic<int64_t> counter = art::now() * 1000000;
thread_local composite query[16]{};

int cmd_ZADD(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;

	if (argc < 4)
		return ValkeyModule_WrongArity(ctx);
	int responses = 0;
	int r = VALKEYMODULE_OK;
	size_t nlen;
	art::zadd_spec zspec(argv, argc);
	if (zspec.parse_options() != VALKEYMODULE_OK)
	{
		return ValkeyModule_WrongArity(ctx);
	}
	int64_t updated = 0;

	auto fc = [&](art::node_ptr) -> void
	{
		++updated;
	};
	const char* key = ValkeyModule_StringPtrLen(argv[1], &nlen);
	if (key_ok(key, nlen) != 0)
	{
		return ValkeyModule_ReplyWithNull(ctx);
	}
	auto before = get_art()->size;
	auto container = conversion::convert(key, nlen);
	query[0].create({container});
	for (int n = zspec.fields_start; n < argc; n+=2)
	{
		size_t klen, vlen;
		const char* k = ValkeyModule_StringPtrLen(argv[n], &klen);
		const char* v = ValkeyModule_StringPtrLen(argv[n + 1], &vlen);

		if (key_ok(k, klen) != 0 || key_ok(v, vlen) != 0)
		{
			r |= ValkeyModule_ReplyWithNull(ctx);
			++responses;
			continue;
		}

		auto score = conversion::convert(k, klen, true);
		//auto member = conversion::convert(v, vlen);
		conversion::comparable_result id {++counter};
		if (score.ctype() != art::tdouble)
		{
			r |= ValkeyModule_ReplyWithNull(ctx);
			++responses;
			continue;
		}
		query[0].push(score);
		//query[0].push(member);
		//query[0].push(id); // so we can add many with the same score
		art::value_type qkey = query[0].create();
		art::value_type qv = {v, (unsigned)vlen};
		if (zspec.XX)
		{
			art::update(get_art(), qkey,[&](const art::node_ptr& old) -> art::node_ptr
			{
				if (old.null()) return nullptr;

				auto l = old.const_leaf();
				return art::make_leaf(qkey, qv, l->ttl(), l->is_volatile());
			});
		}else
		{
			art_insert(get_art(), {}, qkey, qv, !zspec.NX, fc);
		}
		query[0].pop(1);
		++responses;
	}
	auto current = get_art()->size;
	if (zspec.CH)
	{
		ValkeyModule_ReplyWithLongLong(ctx, current - before + updated);
	} else
	{
		ValkeyModule_ReplyWithLongLong(ctx, current - before);
	}

	return 0;
}

int cmd_ZCOUNT(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;
	if (argc < 4)
		return ValkeyModule_WrongArity(ctx);
	size_t nlen,minlen,maxlen;
	const char* n = ValkeyModule_StringPtrLen(argv[1], &nlen);
	const char* smin = ValkeyModule_StringPtrLen(argv[2], &minlen);
	const char* smax = ValkeyModule_StringPtrLen(argv[3], &maxlen);

	if (key_ok(smin, minlen) != 0 || key_ok(smax, maxlen) != 0 || key_ok(n, nlen) != 0)
	{
		return ValkeyModule_ReplyWithNull(ctx);
	}

	auto container = conversion::convert(n, nlen);
	auto mn = conversion::convert(smin, minlen, true);
	auto mx = conversion::convert(smax, maxlen, true);

	auto lower = query[0].create({container,mn});
	auto upper = query[1].create({container,mx});
	long long count = 0;
	art::iterator ai(lower);
	while (ai.ok())
	{
		if (ai.key() <= upper)
		{
			++count;
		}else
		{
			break;
		}
		ai.next();
	}
	return ValkeyModule_ReplyWithLongLong(ctx, count);
}
int cmd_ZCARD(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;
	if (argc < 2)
		return ValkeyModule_WrongArity(ctx);
	size_t nlen;
	const char* n = ValkeyModule_StringPtrLen(argv[1], &nlen);

	if (key_ok(n, nlen) != 0)
	{
		return ValkeyModule_ReplyWithNull(ctx);
	}

	auto container = conversion::convert(n, nlen);

	auto lower = query[0].create({container});
	auto upper = query[1].create({container, art::ts_end});
	long long count = 0;
	art::iterator ai(lower);
	while (ai.ok())
	{
		if (ai.key() <= upper)
		{
			++count;
		}else
		{
			break;
		}
		ai.next();
	}
	return ValkeyModule_ReplyWithLongLong(ctx, count);
}
struct fpair
{
	art::value_type first{nullptr};
	art::value_type second{nullptr};
	fpair() = default;
	fpair(art::value_type first, art::value_type second) : first(first), second(second){}
	fpair(const fpair& other) : first(other.first), second(other.second){}
	fpair& operator=(const fpair& other) = default;

	bool operator < (const fpair& rhs) const
	{
		return first < rhs.first;
	}
};
enum ops
{
	difference = 0,
	intersect = 1,
	uni = 2
};
void z_operate(std::vector<fpair, heap::allocator<fpair>>& result,const conversion::comparable_result& container_a,const conversion::comparable_result& container_b, ops operate)
{
	auto lower_a = query[0].create({container_a});
	auto upper_a = query[1].create({container_a, art::ts_end});
	auto lower_b = query[2].create({container_b});
	auto upper_b = query[3].create({container_b, art::ts_end});
	art::iterator ai(lower_a);
	art::iterator bi(lower_b);
	typedef fpair vt_pair;
	std::vector<vt_pair, heap::allocator<vt_pair>> va;
	std::vector<vt_pair, heap::allocator<vt_pair>> vb;
	;
	while (ai.ok())
	{
		auto v = ai.key();
		if (v >= upper_a) break;
		va.emplace_back(vt_pair{v.sub(lower_a.size, numeric_key_size), ai.value()});
		ai.next();
	}
	while (bi.ok())
	{
		auto v = bi.key();
		if (v >= upper_b) break;
		vb.emplace_back(vt_pair{v.sub(lower_b.size, numeric_key_size), bi.value()});
		bi.next();
	}
	if (operate == intersect)
	{
		std::set_intersection(va.begin(),va.end(),
				vb.begin(),vb.end(),
				back_inserter(result));
	}
	if (operate == difference)
	{
		std::set_intersection(va.begin(),va.end(),
				vb.begin(),vb.end(),
				back_inserter(result));
	}


}
int ZOPER(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc, ops operate)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;
	if (argc < 4)
		return ValkeyModule_WrongArity(ctx);
	long long count;
	if (ValkeyModule_StringToLongLong(argv[1], &count) != VALKEYMODULE_OK)
	{
		return ValkeyModule_ReplyWithNull(ctx);
	}
	size_t alen, blen;
	const char* a = ValkeyModule_StringPtrLen(argv[2], &alen);
	const char* b = ValkeyModule_StringPtrLen(argv[3], &blen);

	if (key_ok(a, alen) != 0 || key_ok(b, blen) != 0)
	{
		return ValkeyModule_ReplyWithNull(ctx);
	}

	auto container_a = conversion::convert(a, alen);
	auto container_b = conversion::convert(b, blen);

	ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
	int64_t replies = 0;
	std::vector<fpair, heap::allocator<fpair>> result;
	z_operate(result, container_a, container_b, operate);
	for (auto& p : result)
	{
		if (reply_encoded_key(ctx, p.first) != VALKEYMODULE_OK)
		{
			ValkeyModule_ReplyWithNull(ctx);
		}
		if (++replies == count) break;
	}
	ValkeyModule_ReplySetArrayLength(ctx, replies);
	return 0;

}
int cmd_ZDIFF(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	return ZOPER(ctx, argv, argc, difference);
}
int cmd_ZINTER(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;
	if (argc < 4)
		return ValkeyModule_WrongArity(ctx);
	art::zops_spec spec(argv,argc);

	if (spec.parse_options() != VALKEYMODULE_OK)
	{
		return ValkeyModule_WrongArity(ctx);
	}
	if (spec.aggr == art::zops_spec::agg_none)
		ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
	long long replies = 0;
	double aggr = 0.0f;
	size_t count = 0;
	size_t ks = spec.keys.size();
	auto fk = spec.keys.begin();
	switch (spec.aggr)
	{
	case art::zops_spec::agg_none:
	case art::zops_spec::avg:
	case art::zops_spec::sum:
		break;
	case art::zops_spec::min:
		aggr = std::numeric_limits<double>::max();
		break;
	case art::zops_spec::max:
		aggr = std::numeric_limits<double>::min();
		break;
	}

	if (spec.aggr == art::zops_spec::min)
	{
		aggr = std::numeric_limits<double>::max();
	}
	if (fk !=  spec.keys.end())
	{
		auto container = conversion::convert(*fk);
		auto lower = query[0].create({container});
		auto upper = query[1].create({container,art::ts_end});
		art::iterator i(lower);

		for (;i.ok();)
		{
			auto v = i.key();
			if (v >= upper) break;

			auto encoded_number = v.sub(lower.size, numeric_key_size);
			auto number = conversion::enc_bytes_to_dbl(encoded_number);

			size_t found_count = 0;
			auto ok = spec.keys.begin();
			++ok;
			for (;ok !=  spec.keys.end();++ok)
			{
				auto check_set = conversion::convert(*ok);
				auto check_tainer = query[2].create({check_set});
				auto check = query[3].create({check_set,conversion::comparable_result(number)});
				art::iterator j(check);
				bool found = false;
				if (j.ok())
				{
					auto kf = j.key();
					auto efn = kf.sub(check_tainer.size, numeric_key_size);
					auto fn = conversion::enc_bytes_to_dbl(efn);
					found = fn==number;
				}
				if (found) found_count++;
			}

			if (count < spec.weight_values.size())
			{
				number *= spec.weight_values[count];
			}
			if (found_count == ks - 1)
			{
				switch (spec.aggr)
				{
				case art::zops_spec::agg_none:
					ValkeyModule_ReplyWithDouble(ctx, number);
					++replies;
					break;
				case art::zops_spec::avg:
				case art::zops_spec::sum:
					aggr += number;
					break;
				case art::zops_spec::min:
					aggr = std::min(number, aggr);
					break;
				case art::zops_spec::max:
					aggr = std::max(number, aggr);
					break;
				}
			}
			++count;
			i.next();
		}

	}
	if (replies == 0 && spec.aggr != art::zops_spec::agg_none)
	{
		return ValkeyModule_ReplyWithDouble(ctx, count > 0 ? aggr : 0.0f);
	}
	ValkeyModule_ReplySetArrayLength(ctx, replies);

	return 0;
}
