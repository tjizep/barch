//
// Created by teejip on 4/15/25.
//
#include "sastam.h"
#include "lzr_log.h"
#if 0
static int test(){
	heap::small_vector<int> testv;
	int ttotal = 0;
	for (int i = 0; i <100; ++i)
	{
		testv.push_back(i);
		ttotal += i;
	}
	int total = 0;
	for (auto r = testv.begin(); r != testv.end(); ++r)
	{
		total += *r;

	}
	if (total != ttotal)
	{
		barch::err({"tesfail"});
	}
	for (auto r = testv.rbegin(); r != testv.rend(); ++r)
	{
		total -= *r;

	}
	if (total != 0)
	{
		barch::err({"tesfail"});
	}
	for (int i = 0; i <100; ++i)
	{
		total += testv.back();
		testv.pop_back();
	}
	if (total != ttotal)
	{
		barch::err({"tesfail"});
	}
	if (!testv.empty())
	{
		barch::err({"should be empty",testv.size()});
	}
	for (int i = 0; i <100; ++i)
	{
		testv.push_back(i);
	}
	int tbefore = total;
	auto t = testv.begin();
	for (int i = 0; i <100; ++i)
	{
		total += *t++;
	}
	if (total != 2*ttotal)
	{
		barch::err({"tesfail"});
	}
	tbefore = total;
	t = testv.begin();
	for (int i = 0; i < 100; ++i)
	{
		total += *t;
		++t;
	}
	if (total != 3*ttotal)
	{
		barch::err({"tesfail"});
	}
	auto tr = testv.rbegin();
	for (int i = 0; i <100; ++i)
	{
		total -= *tr++;
	}

	if (total != tbefore)
	{
		barch::err({"tesfail"});
	}
	total = ttotal;
	auto ttv= testv;
	for (int i = 0; i <100; ++i)
	{
		total -= *ttv.rbegin();
		ttv.pop_back();
	}

	if (total != 0)
	{
		barch::err({"tesfail"});
	}
    return ttotal;
}
static int tests= test();
#endif
