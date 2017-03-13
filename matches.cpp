#include "matches.h"
using namespace Trinity;

// http://stackoverflow.com/questions/2786899/fastest-sort-of-fixed-length-6-int-array
// http://pastebin.com/azzuk072
// http://pages.ripco.net/~jgamble/nw.html
namespace
{
#define SWAP(x, y)                                                                                               \
        if (terms[y].queryTermInstances->instances[0].index < terms[x].queryTermInstances->instances[0].index) \
                std::swap(terms[y], terms[x]);

        static inline void sort3(matched_query_term *terms)
        {
                SWAP(1, 2);

                SWAP(0, 2);

                SWAP(0, 1);
        }

        static inline void sort4(matched_query_term *terms)
        {
                SWAP(0, 1);

                SWAP(2, 3);

                SWAP(0, 2);

                SWAP(1, 3);

                SWAP(1, 2);
        }

        static inline void sort5(matched_query_term *terms)
        {
                SWAP(0, 1);

                SWAP(3, 4);

                SWAP(2, 4);

                SWAP(2, 3);

                SWAP(0, 3);

                SWAP(0, 2);

                SWAP(1, 4);

                SWAP(1, 3);

                SWAP(1, 2);
        }

        static inline void sort6(matched_query_term *terms)
        {
                SWAP(1, 2);

                SWAP(0, 2);

                SWAP(0, 1);

                SWAP(4, 5);

                SWAP(3, 5);

                SWAP(3, 4);

                SWAP(0, 3);

                SWAP(1, 4);

                SWAP(2, 5);

                SWAP(2, 4);

                SWAP(1, 3);

                SWAP(2, 3);
        }

        static inline void sort7(matched_query_term *terms)
        {
                SWAP(1, 2);

                SWAP(0, 2);

                SWAP(0, 1);

                SWAP(3, 4);

                SWAP(5, 6);

                SWAP(3, 5);

                SWAP(4, 6);

                SWAP(4, 5);

                SWAP(0, 4);

                SWAP(0, 3);

                SWAP(1, 5);

                SWAP(2, 6);

                SWAP(2, 5);

                SWAP(1, 3);

                SWAP(2, 4);

                SWAP(2, 3);
        }

        static inline void sort8(matched_query_term *terms)
        {
                SWAP(0, 1);

                SWAP(2, 3);

                SWAP(0, 2);

                SWAP(1, 3);

                SWAP(1, 2);

                SWAP(4, 5);

                SWAP(6, 7);

                SWAP(4, 6);

                SWAP(5, 7);

                SWAP(5, 6);

                SWAP(0, 4);

                SWAP(1, 5);

                SWAP(1, 4);

                SWAP(2, 6);

                SWAP(3, 7);

                SWAP(3, 6);

                SWAP(2, 4);

                SWAP(3, 5);

                SWAP(3, 4);
        }

        static inline void sort9(matched_query_term *terms)
        {
                SWAP(0, 1);

                SWAP(2, 3);

                SWAP(0, 2);

                SWAP(1, 3);

                SWAP(1, 2);

                SWAP(4, 5);

                SWAP(7, 8);

                SWAP(6, 8);

                SWAP(6, 7);

                SWAP(4, 7);

                SWAP(4, 6);

                SWAP(5, 8);

                SWAP(5, 7);

                SWAP(5, 6);

                SWAP(0, 5);

                SWAP(0, 4);

                SWAP(1, 6);

                SWAP(1, 5);

                SWAP(1, 4);

                SWAP(2, 7);

                SWAP(3, 8);

                SWAP(3, 7);

                SWAP(2, 5);

                SWAP(2, 4);

                SWAP(3, 6);

                SWAP(3, 5);

                SWAP(3, 4);
        }

        static inline void sort10(matched_query_term *terms)
        {
                SWAP(0, 1);

                SWAP(3, 4);

                SWAP(2, 4);

                SWAP(2, 3);

                SWAP(0, 3);

                SWAP(0, 2);

                SWAP(1, 4);

                SWAP(1, 3);

                SWAP(1, 2);

                SWAP(5, 6);

                SWAP(8, 9);

                SWAP(7, 9);

                SWAP(7, 8);

                SWAP(5, 8);

                SWAP(5, 7);

                SWAP(6, 9);

                SWAP(6, 8);

                SWAP(6, 7);

                SWAP(0, 5);

                SWAP(1, 6);

                SWAP(1, 5);

                SWAP(2, 7);

                SWAP(3, 8);

                SWAP(4, 9);

                SWAP(4, 8);

                SWAP(3, 7);

                SWAP(4, 7);

                SWAP(2, 5);

                SWAP(3, 6);

                SWAP(4, 6);

                SWAP(3, 5);

                SWAP(4, 5);
        }

        static inline void sort11(matched_query_term *terms)
        {
                SWAP(0, 1);

                SWAP(3, 4);

                SWAP(2, 4);

                SWAP(2, 3);

                SWAP(0, 3);

                SWAP(0, 2);

                SWAP(1, 4);

                SWAP(1, 3);

                SWAP(1, 2);

                SWAP(6, 7);

                SWAP(5, 7);

                SWAP(5, 6);

                SWAP(9, 10);

                SWAP(8, 10);

                SWAP(8, 9);

                SWAP(5, 8);

                SWAP(6, 9);

                SWAP(7, 10);

                SWAP(7, 9);

                SWAP(6, 8);

                SWAP(7, 8);

                SWAP(0, 6);

                SWAP(0, 5);

                SWAP(1, 7);

                SWAP(1, 6);

                SWAP(1, 5);

                SWAP(2, 8);

                SWAP(3, 9);

                SWAP(4, 10);

                SWAP(4, 9);

                SWAP(3, 8);

                SWAP(4, 8);

                SWAP(2, 5);

                SWAP(3, 6);

                SWAP(4, 7);

                SWAP(4, 6);

                SWAP(3, 5);

                SWAP(4, 5);
        }

        static inline void sort12(matched_query_term *terms)
        {
                SWAP(1, 2);

                SWAP(0, 2);

                SWAP(0, 1);

                SWAP(4, 5);

                SWAP(3, 5);

                SWAP(3, 4);

                SWAP(0, 3);

                SWAP(1, 4);

                SWAP(2, 5);

                SWAP(2, 4);

                SWAP(1, 3);

                SWAP(2, 3);

                SWAP(7, 8);

                SWAP(6, 8);

                SWAP(6, 7);

                SWAP(10, 11);

                SWAP(9, 11);

                SWAP(9, 10);

                SWAP(6, 9);

                SWAP(7, 10);

                SWAP(8, 11);

                SWAP(8, 10);

                SWAP(7, 9);

                SWAP(8, 9);

                SWAP(0, 6);

                SWAP(1, 7);

                SWAP(2, 8);

                SWAP(2, 7);

                SWAP(1, 6);

                SWAP(2, 6);

                SWAP(3, 9);

                SWAP(4, 10);

                SWAP(5, 11);

                SWAP(5, 10);

                SWAP(4, 9);

                SWAP(5, 9);

                SWAP(3, 6);

                SWAP(4, 7);

                SWAP(5, 8);

                SWAP(5, 7);

                SWAP(4, 6);

                SWAP(5, 6);
        }

#undef SWAP
}

void Trinity::matched_document::sort_matched_terms_by_query_index()
{
        // See: http://stackoverflow.com/questions/2786899/fastest-sort-of-fixed-length-6-int-array
        typedef void (*sortfunc_t)(matched_query_term *);
        static constexpr sortfunc_t funcs[] =
        {
                nullptr,
                nullptr,
                nullptr,
                sort3,
		sort4,
		sort5,
		sort6,
		sort7,
		sort8,
		sort9,
		sort10,
		sort11,
		sort12
        };

        if (matchedTermsCnt == 1)
                return;
        else if (matchedTermsCnt == 2)
        {
                if (matchedTerms[1].queryTermInstances->instances[0].index < matchedTerms[0].queryTermInstances->instances[0].index)
                        std::swap(matchedTerms[1], matchedTerms[0]);
        }
        else if (matchedTermsCnt < sizeof_array(funcs))
                funcs[matchedTermsCnt](matchedTerms);
        else if (matchedTermsCnt < 16)
        {
                const int32_t n = matchedTermsCnt;

                for (int32_t i{0}, j; i != n; ++i)
                {
                        const auto temp = matchedTerms[i];

                        for (j = i; j > 0 && matchedTerms[j - 1].queryTermInstances->instances[0].index > temp.queryTermInstances->instances[0].index; --j)
                                matchedTerms[j] = matchedTerms[j - 1];
                        {
                                matchedTerms[j] = temp;
                        }
                }
        }
        else
                std::sort(matchedTerms, matchedTerms + matchedTermsCnt, [](const auto &a, const auto &b) { return a.queryTermInstances->instances[0].index < b.queryTermInstances->instances[0].index; });
}
