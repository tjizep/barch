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
	heap::set<size_t> available{};
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
		abort();
	}
	composite& operator[](size_t i)
	{
		if (i >= max_queries_per_call)
		{
			abort();
		}
		return query[i];
	}
	void release(size_t id)
	{
		if (available.contains(id))
		{
			abort();
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
		return ValkeyModule_ReplyWithError(ctx, "syntax error");
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
int cmd_ZINCRBY(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;

	if (argc < 4)
		return ValkeyModule_WrongArity(ctx);
	int responses = 0;
	size_t nlen;
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
	size_t vlen;
	double incr = 0.0f;
	if (VALKEYMODULE_OK != ValkeyModule_StringToDouble(argv[2],&incr))
	{
		return ValkeyModule_ReplyWithError(ctx,"invalid argument");
	}
	const char* v = ValkeyModule_StringPtrLen(argv[3], &vlen);

	if (key_ok(v, vlen) != 0)
	{
		return ValkeyModule_ReplyWithError(ctx,"invalid argument");
	}
	auto target = conversion::convert(v, vlen, true);
	auto target_member = target.get_value();
	auto container = conversion::convert(key, nlen);
	query q1,q2;
	auto prefix = q1->create({container});
	art::iterator scores(prefix);
	// we'll just add a bucket index to make scanning these faster without adding
	// too much data
	while (scores.ok())
	{
		auto k = scores.key();
		auto val = scores.value();
		if (!k.starts_with(prefix)) break;
		auto encoded_number = k.sub(prefix.size, numeric_key_size);
		auto member = k.sub(prefix.size + numeric_key_size);
		if (target_member == member)
		{
			double number = conversion::enc_bytes_to_dbl(encoded_number);
			number += incr;
			conversion::comparable_result id {++counter};
			q1->push(conversion::comparable_result(number));
			q1->push(member);
			art::value_type qkey = q1->create();
			art_insert(get_art(), {}, qkey, val, true, fc);

			q1->pop(2);
			if (!scores.remove()) // remove the current one
			{
				return ValkeyModule_ReplyWithError(ctx,"internal error");
			};

			++responses;
			return ValkeyModule_ReplyWithDouble(ctx, number);
		}

		scores.next();
	}
	if (responses == 0)
	{
		auto score = conversion::comparable_result(incr);
		auto member = conversion::convert(v, vlen);
		q1->push(score);
		q1->push(member);
		art::value_type qkey = q1->create();
		art::value_type qv = {v, (unsigned)vlen};
		art_insert(get_art(), {}, qkey, qv, true, fc);
		q1->pop(2);
		return ValkeyModule_ReplyWithDouble(ctx, incr);
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
	query lq,uq,pq;
	auto lower = lq->create({container,mn});
	auto prefix = pq->create({container});
	auto upper = uq->create({container,mx});
	long long count = 0;
	art::iterator ai(lower);
	while (ai.ok())
	{
		auto ik = ai.key();
		if (!ik.starts_with(prefix)) break;
		if (ik.sub(0,prefix.size + numeric_key_size) <= upper)
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
static int ZRANGE(ValkeyModuleCtx* ctx, const art::zrange_spec& spec)
{
	auto container = conversion::convert(spec.key);
	auto mn = conversion::convert(spec.start, true);
	auto mx = conversion::convert(spec.stop, true);
	query lq,uq,pq;
	auto lower = lq->create({container,mn});
	auto prefix = pq->create({container});
	auto upper = uq->create({container,mx});
	long long count = 0;
	long long replies = 0;
	heap::std_vector<std::pair<art::value_type,art::value_type>> bylex;
	heap::std_vector<art::value_type> rev;
	ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
	art::iterator ai(lower);
	while (ai.ok())
	{
		auto v = ai.key();
		if (!v.starts_with(prefix)) break;
		if (v.sub(0,prefix.size + numeric_key_size) <= upper)
		{
			bool doprint = !spec.count;

			if (spec.count && count >= spec.offset && (count - spec.offset < spec.count))
			{
				doprint = true;
			}
			if (doprint)
			{
				auto encoded_number = v.sub(prefix.size, numeric_key_size);
				auto member = v.sub(prefix.size + numeric_key_size);
				bool pushed = spec.BYLEX || spec.REV;
				if (spec.BYLEX)
				{
					bylex.push_back({member,encoded_number});
				}
				if (spec.REV && !spec.BYLEX)
				{
					rev.push_back(v.sub(prefix.size, numeric_key_size * 2));
				}
				if (!pushed)
				{
					reply_encoded_key(ctx, member);
					++replies;
					if (spec.has_withscores)
					{
						reply_encoded_key(ctx, encoded_number);
						++replies;
					}
				}
			}
			++count;
		}else
		{
			break;
		}
		ai.next();
	}
	if (spec.BYLEX)
	{	if (spec.REV)
		{
			std::sort(bylex.begin(), bylex.end(),[](auto& a, auto& b)
			{
				return b < a;
			});
		}
		for (auto& rec: bylex)
		{
			reply_encoded_key(ctx, rec.first);
			++replies;
			if (spec.has_withscores)
			{
				reply_encoded_key(ctx, rec.second);
				++replies;
			}
		}
	}else if (spec.REV)
	{
		std::sort(rev.begin(), rev.end(),[](auto& a, auto& b)
		{
			return b < a;
		});
		for (auto& rec: rev)
		{
			reply_encoded_key(ctx, rec.sub(numeric_key_size ,numeric_key_size));
			++replies;
			if (spec.has_withscores)
			{
				reply_encoded_key(ctx, rec.sub(0 ,numeric_key_size));
				++replies;
			}
		}
	};
	ValkeyModule_ReplySetArrayLength(ctx, replies);

	return 0;
}

int cmd_ZRANGE(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;
	if (argc < 4)
		return ValkeyModule_WrongArity(ctx);
	art::zrange_spec spec(argv,argc);
	if (spec.parse_options() != VALKEYMODULE_OK)
	{
		return ValkeyModule_ReplyWithError(ctx, "syntax error");
	}

	return ZRANGE(ctx, spec);
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
		if (!ai.key().starts_with(lower)) break;
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

static int ZOPER(
	ValkeyModuleCtx* ctx,
	ValkeyModuleString** argv,
	int argc,
	ops operate,
	std::string store = "", bool card = false)
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
	if (spec.aggr == art::zops_spec::agg_none && store.empty())
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
		int64_t updated = 0;
		auto fc = [&](art::node_ptr) -> void
		{
			++updated;
		};
		query lq,uq,q1;
		auto container = conversion::convert(*fk);
		auto lower = lq->create({container});
		if (!store.empty())
			q1->create({conversion::convert(store)});

		art::iterator i(lower);

		for (;i.ok();)
		{
			auto v = i.key();
			if (!v.starts_with(lower)) break;

			auto encoded_number = v.sub(lower.size, numeric_key_size);
			auto member = v.sub(lower.size + numeric_key_size); // theres a 0 char and I'm not sure where it comes from
			auto number = conversion::enc_bytes_to_dbl(encoded_number);

			size_t found_count = 0;
			auto ok = spec.keys.begin();
			++ok;

			for (;ok !=  spec.keys.end();++ok)
			{
				query tainerq, checkq;
				auto check_set = conversion::convert(*ok);
				auto check_tainer = tainerq->create({check_set});
				auto check = checkq->create({check_set, conversion::comparable_result(number)});
				art::iterator j(check);
				bool found = false;
				if (j.ok())
				{
					auto kf = j.key();
					if (!kf.starts_with(check)) break;

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
					if (store.empty())
					{
						reply_encoded_key(ctx, member);
						++replies;
						if (spec.has_withscores)
						{
							//ValkeyModule_ReplyWithDouble(ctx, number);
							reply_encoded_key(ctx, encoded_number);
							++replies;
						}

					}else
					{
						// this is possible because the art tree is guaranteed not to reallocate
						// anything during the compressed_release scope
						if (!card)
						{
							conversion::comparable_result id {++counter};

							q1->push({encoded_number});
							q1->push({member});
							art::value_type qkey = q1->create();
							art::value_type qv = i.value();
							art_insert(get_art(), {}, qkey, qv, false, fc);

							q1->pop(2);
						}
						replies++;
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
	if (store.empty())
	{
		ValkeyModule_ReplySetArrayLength(ctx, replies);
	}else
	{
		return ValkeyModule_ReplyWithLongLong(ctx, replies);
	}


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

int cmd_ZDIFFSTORE(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	size_t mlen = 0;
	const char * member = ValkeyModule_StringPtrLen(argv[1],&mlen);
	if (mlen == 0)
		return ValkeyModule_ReplyWithError(ctx,"syntax error");
	return ZOPER(ctx, &argv[1], argc-1, difference,member);
}
int cmd_ZINTERSTORE(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	size_t mlen = 0;
	const char * member = ValkeyModule_StringPtrLen(argv[1],&mlen);
	if (mlen == 0)
		return ValkeyModule_ReplyWithError(ctx,"syntax error");
	return ZOPER(ctx, &argv[1], argc-1, intersect,member);
}
int cmd_ZINTERCARD(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{

	return ZOPER(ctx, argv, argc, intersect,"#",true);
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

int cmd_ZPOPMIN(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;

	if (argc < 2)
		return ValkeyModule_WrongArity(ctx);
	size_t klen;
	long long count = 1;
	long long replies = 0;
	const char* k = ValkeyModule_StringPtrLen(argv[1], &klen);
	if (argc == 3)
	{
		if (VALKEYMODULE_OK != ValkeyModule_StringToLongLong(argv[2], &count))
		{
			return ValkeyModule_ReplyWithError(ctx,"invalid count");
		}
	}

	if (key_ok(k, klen) != 0)
	{
		return ValkeyModule_ReplyWithNull(ctx);
	}
	auto container = conversion::convert(k, klen);
	query l,u;
	auto lower = l->create({container});
	ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);

	for (long long c = 0; c < count; ++c)
	{
		art::iterator i(lower);
		if (!i.ok())
		{
			break;
		}
		auto v = i.key();
		if (!v.starts_with(lower)) break;
		auto encoded_number = v.sub(lower.size, numeric_key_size);
		auto member = v.sub(lower.size + numeric_key_size); // theres a 0 char and I'm not sure where it comes from
		reply_encoded_key(ctx, encoded_number);
		reply_encoded_key(ctx, member);
		replies += 2;
		if (!i.remove())
		{
			break;
		};
	}
	ValkeyModule_ReplySetArrayLength(ctx, replies);
	return 0;
}

int cmd_ZPOPMAX(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;

	if (argc < 2)
		return ValkeyModule_WrongArity(ctx);
	size_t klen;
	long long count = 1;
	long long replies = 0;
	const char* k = ValkeyModule_StringPtrLen(argv[1], &klen);
	if (argc == 3)
	{
		if (VALKEYMODULE_OK != ValkeyModule_StringToLongLong(argv[2], &count))
		{
			return ValkeyModule_ReplyWithError(ctx,"invalid count");
		}
	}

	if (key_ok(k, klen) != 0)
	{
		return ValkeyModule_ReplyWithNull(ctx);
	}

	auto container = conversion::convert(k, klen);
	query l,u;
	auto lower = l->create({container});
	auto upper = u->create({container, art::ts_end});
	ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);

	for (long long c = 0; c < count; ++c)
	{
		art::iterator i(upper);
		if (!i.ok())
		{
			break;
		}
		auto v = i.key();
		if (!v.starts_with(lower)) {
			i.previous();
			if (!i.ok())
			{
				break;
			}
			v = i.key();
		};
		if (!v.starts_with(lower)) break;

		auto encoded_number = v.sub(lower.size, numeric_key_size);
		auto member = v.sub(lower.size + numeric_key_size); // theres a 0 char and I'm not sure where it comes from
		reply_encoded_key(ctx, encoded_number);
		reply_encoded_key(ctx, member);
		replies += 2;

		if (!i.remove())
		{
			art::std_log("Could not remove key");
		};
	}
	ValkeyModule_ReplySetArrayLength(ctx, replies);
	return 0;
}

int cmd_ZREVRANGE(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;
	if (argc < 4)
		return ValkeyModule_WrongArity(ctx);
	art::zrange_spec spec(argv,argc);
	if (spec.parse_options() != VALKEYMODULE_OK)
	{
		return ValkeyModule_ReplyWithError(ctx, "syntax error");
	}
	spec.REV = true;
	spec.BYLEX = false;
	return ZRANGE(ctx, spec);
}

int cmd_ZRANGEBYSCORE(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;
	if (argc < 4)
		return ValkeyModule_WrongArity(ctx);
	art::zrange_spec spec(argv,argc);
	if (spec.parse_options() != VALKEYMODULE_OK)
	{
		return ValkeyModule_ReplyWithError(ctx, "syntax error");
	}
	spec.REV = false;
	spec.BYLEX = false;
	return ZRANGE(ctx, spec);
}
int cmd_ZREVRANGEBYSCORE(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;
	if (argc < 4)
		return ValkeyModule_WrongArity(ctx);
	art::zrange_spec spec(argv,argc);
	if (spec.parse_options() != VALKEYMODULE_OK)
	{
		return ValkeyModule_ReplyWithError(ctx, "syntax error");
	}
	spec.REV = true;
	spec.BYLEX = false;
	return ZRANGE(ctx, spec);
}
int cmd_ZRANGEBYLEX(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;
	if (argc < 4)
		return ValkeyModule_WrongArity(ctx);
	art::zrange_spec spec(argv,argc);
	if (spec.parse_options() != VALKEYMODULE_OK)
	{
		return ValkeyModule_ReplyWithError(ctx, "syntax error");
	}
	spec.REV = false;
	spec.BYLEX = true;
	return ZRANGE(ctx, spec);
}
int cmd_ZREVRANGEBYLEX(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;
	if (argc < 4)
		return ValkeyModule_WrongArity(ctx);
	art::zrange_spec spec(argv,argc);
	if (spec.parse_options() != VALKEYMODULE_OK)
	{
		return ValkeyModule_ReplyWithError(ctx, "syntax error");
	}
	spec.REV = true;
	spec.BYLEX = true;
	return ZRANGE(ctx, spec);
}

int add_ordered_api(ValkeyModuleCtx* ctx)
{

	if (ValkeyModule_CreateCommand(ctx, NAME(ZPOPMIN), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZPOPMAX), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZADD), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZCOUNT), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZCARD), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZDIFF), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZDIFFSTORE), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZINTERSTORE), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZINCRBY), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZINTERCARD), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZINTER), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZRANGE), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZREVRANGE), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZRANGEBYSCORE), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZREVRANGEBYSCORE), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZREVRANGEBYLEX), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZRANGEBYLEX), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}