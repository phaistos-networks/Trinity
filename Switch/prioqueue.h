#pragma once
#include <switch.h>

// A simple binary heap / prio.queue, of fixed capacity_.
// It's more flexible and faster than std::priority_queue<>, and it also
// supports faster top value updates via update_top(). It was 32% on a specific use case benchmarked against std::priority_queue<>
//
// base-1 not base-0, because it affords some nice optimizations
// Based on Lucene's PriorityQueue design and impl.
//
// XXX:  the provided Compare class operator() should return true if first passed < second passed, so that
// top() always contains the lowest value, UNLIKE std::priority_queue where the respective Compare class operator()
// needs to check if second < first in order to get the same order
namespace Switch
{
        template <typename T, class Compare = std::less<T>>
        class priority_queue
        {
              public:
                using const_reference = const T &;
                using reference = T &;
                using value_type = T;

              private:
                const uint32_t capacity_;
                uint32_t size_{0};
                T *const heap;

              private:
                bool up(const uint32_t orig)
                {
                        auto i{orig};
                        const auto node = heap[i];
                        const Compare cmp;

                        for (uint32_t j = i / 2; j > 0 && cmp(node, heap[j]); j /= 2)
                        {
                                heap[i] = heap[j]; // shift parents down
                                i = j;
                        }

                        heap[i] = node; // install saved node
                        return i != orig;
                }

                void down(uint32_t i)
                {
                        const auto node = heap[i]; // top node
                        uint32_t j = i << 1;       // smaller child(left)
                        uint32_t k = j + 1;        // (right)
                        const Compare cmp;

                        if (k <= size_ && cmp(heap[k], heap[j]))
                        {
                                // right child exists and smaller than left
                                j = k;
                        }

                        while (j <= size_ && cmp(heap[j], node))
                        {
                                heap[i] = heap[j]; // shift up child
                                i = j;
                                j = i << 1;
                                k = j + 1;

                                if (k <= size_ && cmp(heap[k], heap[j]))
                                        j = k;
                        }

                        heap[i] = node;
                }

              public:
                priority_queue(const uint32_t m)
                    : capacity_{m}, heap((T *)malloc(sizeof(T) * (0 == capacity_ ? 2 : capacity_ + 1))) // base-1 based, heap[0] is not used
                {
                        if (unlikely(!heap))
                                throw Switch::data_error("Failed to allocate memory");
                }

                ~priority_queue()
                {
                        std::free(heap);
                }

                void clear()
                {
                        size_ = 0;
                }

                // Attempts to push a new value
                // It returns true if a value was dropped to make space for it, or `v` can't be inserted due to capacity_ constraints
                // if true is returned, prev is assigned either v or the value that was dropped to make space
                bool try_push(const T v, T &prev)
                {
                        if (size_ < capacity_)
                        {
                                push(v);
                                return false;
                        }
                        else if (size_ && Compare{}(v, heap[1]))
                        {
                                prev = std::move(heap[1]);
                                heap[1] = std::move(v);
                                update_top();
                                return true;
                        }
                        else
                        {
                                prev = v;
                                return true;
                        }
                }

                void push(const T &v)
                {
#if 0 // use try_push() if you need capacity_ checks
                        if (unlikely(size_ == capacity_))
                                throw Switch::data_error("Full");
#endif

                        heap[++size_] = v;
                        up(size_);
                }

                void push(T &&v)
                {
#if 0
                        if (unlikely(size_ == capacity_))
                                throw Switch::data_error("Full");
#endif

                        heap[++size_] = std::move(v);
                        up(size_);
                }

		// push_back() does NOT ensure the heap semantics are upheld
		// this is really only useful if you want to e.g populate a pq until
		// you reach a size and then use make_heap() and from then on, you use push()/pop()
		// Make sure you know what you are doing here
		void push_back(T &&v)
		{
			heap[++size_] = std::move(v);
		}

		void push_back(const T &v)
		{
			heap[++size_] = v;
		}

		void make_heap()
                {
                        struct
                        {
                                inline bool operator()(const T &a, const T &b) noexcept
                                {
					// we are comparing (b,a), not (a,b) because
					// the semantics are inverted in STL
                                        return Compare{}(b, a);
                                }
                        } hp;

                        std::make_heap(heap + 1, heap + 1 + size_, hp);
                }

                T pop() noexcept
                {
                        // won't check if (size == 0), so that we can use noexcept
                        // but you should check
                        const auto res = heap[1];

                        heap[1] = std::move(heap[size_--]); // remember, base-1
                        down(1);
                        return res;
                }

		constexpr auto capacity() const noexcept
		{
			return capacity_;
		}

                constexpr auto size() const noexcept
                {
                        return size_;
                }

                constexpr auto empty() const noexcept
                {
                        return 0 == size_;
                }

                // Not sure why you 'd need data() though, because
                // it will be out of order, but here it is (maybe you want to
                // store the values somewhere without draining the queue)
                constexpr auto data() const noexcept
                {
                        return heap + 1; // base 1
                }

                constexpr auto data() noexcept
                {
                        return heap + 1; // base 1
                }

                [[gnu::always_inline]] const_reference top() const noexcept
                {
                        // not check if (size == 0)
                        // so we can use noexcept, but make sure you know what you are doing
                        return heap[1];
                }

                [[gnu::always_inline]] reference top() noexcept
                {
                        return heap[1];
                }

                // invoke when the top changes value
                // still log(n) worse case, but it's x2 faster compared to
                // { auto v = pop(); o.update(); push(o); }
                [[gnu::always_inline]] void update_top() noexcept
                {
                        down(1);
                }

                inline void update_top(const_reference v)
                {
                        heap[1] = v;
                        down(1);
                }

                inline void update_top(T &&v)
                {
                        heap[1] = std::move(v);
                        down(1);
                }

                // Removes an existing value. Cost is linear with the size of queue.
                bool erase(const T v)
                {
                        for (uint32_t i{1}; i <= size_; ++i)
                        {
                                if (heap[i] == v)
                                {
                                        heap[i] = std::move(heap[size_--]);
                                        if (i <= size_)
                                        {
                                                if (!up(i))
                                                {
                                                        down(i);
                                                }
                                        }
                                        return true;
                                }
                        }
                        return false;
                }
        };
}
