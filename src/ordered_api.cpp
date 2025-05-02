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
#define IX_MEMBER ""
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
	composite * cache = &queries[id];
	composite* operator->() const
	{
		return cache;//&queries[id];
	}
	~query()
	{
		queries.release(id);
	}
};
struct ordered_keys
{
	ordered_keys(const ordered_keys&) = default;
	ordered_keys(ordered_keys&&) = default;
	ordered_keys(
		const composite& score_key,
		const composite& member_key,
		art::value_type value) : score_key(score_key), member_key(member_key), value(value) {}

	composite score_key;
	composite member_key;
	art::value_type value;
};
void insert_ordered(composite& score_key, composite& member_key, art::value_type value, bool update = false)
{
	auto sk = score_key.create();
	auto mk = member_key.create();
	art_insert(get_art(), sk, value,update,[](art::node_ptr) -> void{});
	art_insert(get_art(), mk, sk,update,[](art::node_ptr) -> void{});
}
void remove_ordered(composite& score_key, composite& member_key)
{
	auto sk = score_key.create();
	auto mk = member_key.create();
	art_delete(get_art(), sk);
	art_delete(get_art(), mk);
}
void insert_ordered(ordered_keys& thing, bool update = false)
{
	insert_ordered(thing.score_key, thing.member_key, thing.value, update);
}
void remove_ordered(ordered_keys& thing)
{
	remove_ordered(thing.score_key, thing.member_key);
}
static composite cmd_ZADD_q1;
static composite cmd_ZADD_qindex;
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
	zspec.LFI = true;
	int64_t updated = 0;
	int64_t fkadded = 0;
	auto rt = get_art();
	auto fc = [&](art::node_ptr) -> void
	{
		++updated;
	};
	auto fcfk = [&](art::node_ptr val) -> void
	{
		if (val.is_leaf)
		{
			art_delete(rt, val.const_leaf()->get_value());

		}
		--fkadded;

	};
	const char* key = ValkeyModule_StringPtrLen(argv[1], &nlen);
	if (key_ok(key, nlen) != 0)
	{
		return ValkeyModule_ReplyWithNull(ctx);
	}

	auto before = rt->size;
	auto container = conversion::convert(key, nlen);
	for (int n = zspec.fields_start; n < argc; n+=2)
	{
		size_t klen, vlen;
		const char* k = ValkeyModule_StringPtrLen(argv[n], &klen);
		if (n + 1 >= argc)
		{
			return ValkeyModule_ReplyWithError(ctx, "syntax error");
		}
		const char* v = ValkeyModule_StringPtrLen(argv[n + 1], &vlen);

		if (key_ok(k, klen) != 0 || key_ok(v, vlen) != 0)
		{
			r |= ValkeyModule_ReplyWithNull(ctx);
			++responses;
			continue;
		}

		auto score = conversion::convert(k, klen, true);
		auto member = conversion::convert(v, vlen);
		conversion::comparable_key id {++counter};
		if (score.ctype() != art::tfloat && score.ctype() != art::tdouble)
		{
			r |= ValkeyModule_ReplyWithNull(ctx);
			++responses;
			continue;
		}
		art::value_type qkey = cmd_ZADD_q1.create({container, score, member});
		if (zspec.XX)
		{
			art::value_type qkey = cmd_ZADD_q1.create({container, score,member});
			art::update(rt, qkey,[&](const art::node_ptr& old) -> art::node_ptr
			{
				if (old.null()) return nullptr;

				auto l = old.const_leaf();
				return art::make_leaf(qkey, {}, l->ttl(), l->is_volatile());
			});
		}else
		{
			if (zspec.LFI)
			{
				auto member_key = cmd_ZADD_qindex.create({IX_MEMBER ,container, member});//, score
				art_insert(rt, member_key, qkey, true, fcfk);
				++fkadded;
			}

			art_insert(rt, qkey, {}, !zspec.NX, fc);

		}
		++responses;
	}
	auto current = rt->size;
	if (zspec.CH)
	{
		ValkeyModule_ReplyWithLongLong(ctx, current - before + updated - fkadded);
	} else
	{
		ValkeyModule_ReplyWithLongLong(ctx, current - before - fkadded);
	}

	return 0;
}
int cmd_ZREM(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;

	if (argc < 3)
		return ValkeyModule_WrongArity(ctx);
	int responses = 0;
	int r = VALKEYMODULE_OK;
	size_t nlen;
	int64_t removed = 0;
	const char* key = ValkeyModule_StringPtrLen(argv[1], &nlen);
	if (key_ok(key, nlen) != 0)
	{
		return ValkeyModule_ReplyWithNull(ctx);
	}
	auto container = conversion::convert(key, nlen);
	query q1,qmember;
	q1->create({container});
	auto member_prefix = qmember->create({IX_MEMBER ,container});
	for (int n = 2; n < argc; ++n)
	{
		size_t mlen = 0;
		const char* mem = ValkeyModule_StringPtrLen(argv[n], &mlen);

		if (key_ok(mem, mlen) != 0)
		{
			r |= ValkeyModule_ReplyWithNull(ctx);
			++responses;
			continue;
		}

		auto member = conversion::convert(mem, mlen);
		conversion::comparable_key id {++counter};
		qmember->push(member);
		art::iterator byscore(qmember->create());
		if (byscore.ok()) {
			auto kscore = byscore.key();
			if (!kscore.starts_with(member_prefix)) break;
			auto fkmember = byscore.value();
			art_delete(get_art(), fkmember);
			if (byscore.remove())
			{
				++removed;
			}
		}
		qmember->pop(1);
		++responses;
	}

	return ValkeyModule_ReplyWithLongLong(ctx, removed);
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
			conversion::comparable_key id {++counter};
			q1->push(conversion::comparable_key(number));
			q1->push(member);
			art::value_type qkey = q1->create();
			art_insert(get_art(), qkey, val, true, fc);

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
		auto score = conversion::comparable_key(incr);
		auto member = conversion::convert(v, vlen);
		q1->push(score);
		q1->push(member);
		art::value_type qkey = q1->create();
		art::value_type qv = {v, (unsigned)vlen};
		art_insert(get_art(), qkey, qv, true, fc);
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
	query lq,uq,pq,tq;
	art::value_type lower;
	art::value_type prefix;
	art::value_type nprefix;
	art::value_type upper;
	if (spec.BYLEX)
	{
		// it is implied that mn and mx are non-numeric strings
		lower = lq->create({IX_MEMBER,container,mn });
		prefix = pq->create({IX_MEMBER, container});
		nprefix = tq->create({container});
		upper = uq->create({IX_MEMBER, container,mx });

	}else
	{
		lower = lq->create({container,mn });
		prefix = pq->create({container});
		nprefix = prefix;
		upper = uq->create({container,mx });
	}
	long long count = 0;
	long long replies = 0;
	heap::std_vector<std::pair<art::value_type,art::value_type>> bylex;
	heap::std_vector<art::value_type> rev;
	heap::vector<ordered_keys> removals;
	if (!spec.REMOVE)
		ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
	art::iterator ai(lower);
	while (ai.ok())
	{
		auto v = ai.key();
		if (!v.starts_with(prefix))
		{
			break;
		}
		art::value_type current_comp;
		if (spec.BYLEX)
		{
			current_comp = v.sub(0, prefix.size + mx.get_size()-1);
			v = ai.value();// in case of bylex the value is a fk to by-score
		}else
		{	current_comp = v.sub(0, prefix.size + numeric_key_size);
		}

		if ( current_comp <= upper )
		{
			bool doprint = !spec.count;

			if (spec.count && count >= spec.offset && (count - spec.offset < spec.count))
			{
				doprint = true;
			}
			if (doprint)
			{
				auto encoded_number = v.sub(nprefix.size, numeric_key_size);
				auto member = v.sub(nprefix.size + numeric_key_size);
				bool pushed = false;

				if (spec.REV && !spec.REMOVE)
				{
					if (spec.BYLEX)
					{
						bylex.push_back({member,encoded_number});
						pushed = true;
					}else
					{
						rev.push_back(v.sub(nprefix.size, numeric_key_size * 2));
						pushed = true;
					}
				}
				if (!pushed && spec.REMOVE) // scheduled for removal
				{
					composite score_key, member_key;
					score_key.create({container,encoded_number,member});
					// fyi: member key means lex key
					member_key.create({IX_MEMBER,container,member});
					removals.push_back({score_key,member_key,art::value_type()});
				}
				if (!pushed && !spec.REMOVE) // bylex should be in correct order
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
	if (spec.BYLEX && !spec.REMOVE)
	{	if (spec.REV)
		{
			std::sort(bylex.begin(), bylex.end(),[](auto& a, auto& b)
			{
				return b < a;
			});
		}
		for (auto& rec: bylex)
		{
			/// TODO: min max filter
			reply_encoded_key(ctx, rec.first);
			++replies;
			if (spec.has_withscores)
			{
				reply_encoded_key(ctx, rec.second);
				++replies;
			}
		}
	}else if (spec.REV && !spec.REMOVE)
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
	if (!spec.REMOVE)
	{
		ValkeyModule_ReplySetArrayLength(ctx, replies);
	}
	else
	{	for (auto& r: removals)
		{
			remove_ordered(r.score_key, r.member_key);
		}
		return ValkeyModule_ReplyWithLongLong(ctx, removals.size());
	}

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
#if 0
static double rnd(float f) {
	return std::round((double)f * 100000.0) / 100000.0;
}
#endif
static double rnd(double f) {
	return f;
}
static int ZOPER(
	ValkeyModuleCtx* ctx,
	ValkeyModuleString** argv,
	int argc,
	ops operate,
	std::string store = "",
	bool card = false,
	bool removal = false)
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
	heap::vector<ordered_keys> new_keys;
	heap::vector<ordered_keys> removed_keys;

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
				auto check = checkq->create({check_set, conversion::comparable_key(number)});
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
							composite score_key, member_key;
							if (removal)
							{
								score_key.create({container,encoded_number,member});
								member_key.create({IX_MEMBER,container,member});
								removed_keys.push_back({score_key,member_key,i.value()});
							}else
							{
								score_key.create({conversion::convert(store),encoded_number,member});
								member_key.create({IX_MEMBER,conversion::convert(store),member});
								new_keys.push_back({score_key,member_key,i.value()});
							}

						}
						replies++;
					}

					break;
				case art::zops_spec::avg:
				case art::zops_spec::sum:
					aggr += rnd(number);
					break;
				case art::zops_spec::min:
					aggr = std::min(rnd(number), aggr);
					break;
				case art::zops_spec::max:
					aggr = std::max(rnd(number), aggr);
					break;
				}
			}
			++count;
			i.next();
		}

	}

	for (auto&ordered_keys : new_keys)
	{
		insert_ordered(ordered_keys);
	}
	for (auto&ordered_keys : removed_keys)
	{
		remove_ordered(ordered_keys);
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
int cmd_ZREMRANGEBYLEX(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
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
	spec.REMOVE = true;
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
int cmd_ZRANK(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;
	if (argc != 4)
	{
		return ValkeyModule_WrongArity(ctx);
	}
	size_t cl=0,al=0,bl=0 ;
	const char * c = ValkeyModule_StringPtrLen(argv[1],&cl);
	if (cl==0)
	{
		return ValkeyModule_WrongArity(ctx);
	}
	const char * a = ValkeyModule_StringPtrLen(argv[2],&al);
	if (al==0)
	{
		return ValkeyModule_WrongArity(ctx);
	}
	const char * b = ValkeyModule_StringPtrLen(argv[3],&bl);
	if (bl==0)
	{
		return ValkeyModule_WrongArity(ctx);
	}

	composite qlower,qupper;
	auto container = conversion::convert(c,cl);
	auto lower = conversion::convert(a,al,true);
	auto upper = conversion::convert(b,bl,true);
	auto min_key = qlower.create({container,lower});
	auto max_key = qupper.create({container,upper});
	if (max_key < min_key)
	{
		return ValkeyModule_ReplyWithLongLong(ctx,0);
	}
	art::iterator first(min_key);

	int64_t rank = 0;
	if (first.ok())
	{

		rank = first.distance(max_key);
	}

	return ValkeyModule_ReplyWithLongLong(ctx,rank);
}

int cmd_ZFASTRANK(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
	ValkeyModule_AutoMemory(ctx);
	compressed_release release;
	if (argc != 4)
	{
		return ValkeyModule_WrongArity(ctx);
	}
	size_t cl=0,al=0,bl=0 ;
	const char * c = ValkeyModule_StringPtrLen(argv[1],&cl);
	if (cl==0)
	{
		return ValkeyModule_WrongArity(ctx);
	}
	const char * a = ValkeyModule_StringPtrLen(argv[2],&al);
	if (al==0)
	{
		return ValkeyModule_WrongArity(ctx);
	}
	const char * b = ValkeyModule_StringPtrLen(argv[3],&bl);
	if (bl==0)
	{
		return ValkeyModule_WrongArity(ctx);
	}

	composite qlower,qupper;
	auto container = conversion::convert(c,cl);
	auto lower = conversion::convert(a,al,true);
	auto upper = conversion::convert(b,bl,true);
	auto min_key = qlower.create({container,lower});
	auto max_key = qupper.create({container,upper});
	if (max_key < min_key)
	{
		return ValkeyModule_ReplyWithLongLong(ctx,0);
	}

	art::iterator first(min_key);
	art::iterator last(max_key);

	int64_t rank = 0;
	if (first.ok() && last.ok())
	{

		rank += last.key() == max_key ? 1 : 0;
		rank += first.fast_distance(last);

	}

	return ValkeyModule_ReplyWithLongLong(ctx,rank);
}

int add_ordered_api(ValkeyModuleCtx* ctx)
{

	if (ValkeyModule_CreateCommand(ctx, NAME(ZPOPMIN), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZPOPMAX), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZADD), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZREM), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
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

	if (ValkeyModule_CreateCommand(ctx, NAME(ZREMRANGEBYLEX), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
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

	if (ValkeyModule_CreateCommand(ctx, NAME(ZRANK), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

	if (ValkeyModule_CreateCommand(ctx, NAME(ZFASTRANK), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
		return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}