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

struct query_pool
{
	composite query[max_queries_per_call]{};
	ankerl::unordered_dense::set<size_t> available{};
	query_pool()
	{
		for (size_t i = 0; i < max_queries_per_call; i++)
		{
			available.insert(i);
		}
	}
	size_t create()
	{	if (!available.empty())
		{
			size_t r = *available.begin();
			available.erase(r);
			return r;
		}
		throw std::runtime_error("Queries overflow");
	}
	composite& operator[](size_t i)
	{
		if (i >= max_queries_per_call)
		{
			throw std::out_of_range("");
		}
		return query[i];
	}
	void release(size_t id)
	{
		if (available.contains(id))
		{
			throw std::out_of_range("");
		}
		available.insert(id);
	}
};
thread_local query_pool queries;
struct query
{
	size_t id = queries.create();
	composite* operator->() const
	{
		return &queries[id];
	}
	~query()
	{
		queries.release(id);
	}
};

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
	query q1,q2;
	q1->create({container});
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
		auto member = conversion::convert(v, vlen);
		conversion::comparable_result id {++counter};
		if (score.ctype() != art::tdouble)
		{
			r |= ValkeyModule_ReplyWithNull(ctx);
			++responses;
			continue;
		}
		q1->push(score);
		q1->push(member);
		//query[0].push(id); // so we can add many with the same score
		art::value_type qkey = q1->create();
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
		q1->pop(2);
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
	query lq,uq;
	auto lower = lq->create({container,mn});
	auto upper = uq->create({container,mx});
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
	query lq,uq;
	auto lower = lq->create({container});
	auto upper = uq->create({container, art::ts_end});
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
enum ops
{
	difference = 0,
	intersect = 1,
	onion = 2
};

int ZOPER(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc, ops operate)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;

	if (argc < 4)
		return ValkeyModule_WrongArity(ctx);
	art::zops_spec spec(argv,argc);

	if (spec.parse_options() != VALKEYMODULE_OK)
	{
		return ValkeyModule_ReplyWithError(ctx,"syntax error");
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
		query lq,uq;
		auto container = conversion::convert(*fk);
		auto lower = lq->create({container});
		auto upper = uq->create({container,art::ts_end});
		art::iterator i(lower);

		for (;i.ok();)
		{
			auto v = i.key();
			if (v >= upper) break;

			auto encoded_number = v.sub(lower.size, numeric_key_size);
			auto member = v.sub(lower.size + numeric_key_size); // theres a 0 char and I'm not sure where it comes from
			auto number = conversion::enc_bytes_to_dbl(encoded_number);

			size_t found_count = 0;
			auto ok = spec.keys.begin();
			++ok;

			for (;ok !=  spec.keys.end();++ok)
			{
				query tainerq,checkq;
				auto check_set = conversion::convert(*ok);
				auto check_tainer = tainerq->create({check_set});
				auto check = checkq->create({check_set,conversion::comparable_result(number)});
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
			bool add_result = false;
			switch (operate)
			{
			case intersect:
				add_result = (found_count == ks - 1);
				break;
			case difference:
				add_result = (found_count == 0);
				break;
			case onion: // union lol, does not work yet

				break;
			}
			if (add_result)
			{
				switch (spec.aggr)
				{
				case art::zops_spec::agg_none:
					reply_encoded_key(ctx, member);
					++replies;
					if (spec.has_withscores)
					{
						//ValkeyModule_ReplyWithDouble(ctx, number);
						reply_encoded_key(ctx, encoded_number);
						++replies;
					}

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
int cmd_ZDIFF(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	try
	{
		return ZOPER(ctx, argv, argc, difference);
	}catch (std::exception& e)
	{
		art::log(e,__FILE__,__LINE__);
	}
	return ValkeyModule_ReplyWithError(ctx,"internal error");

}
int cmd_ZINTER(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	try
	{
		return ZOPER(ctx, argv, argc, intersect);
	}catch (std::exception& e)
	{
		art::log(e,__FILE__,__LINE__);
	}
	return ValkeyModule_ReplyWithError(ctx,"internal error");
}
