#pragma once
#include "common.h"

#ifdef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wundefined-inline"
#endif

namespace Trinity {
        // Currently, we don't really need to query for total matches in the current document
        // but if we do, or you need it in your handler for whatever reason, uncomment RDP_NEED_TOTAL_MATCHES
        // and rebuild. This is not enabled because inflating the vtables is not worth it for a call that
        // you may not use.
        //
        // This, along with moving score() out of relevant_document_provider is done so that we
        // reduce the vtable size of Iterator and thereby reduce cache-misses.
        //
        // We could probably move Iterator::curDocument into relevant_document_provider thereby not having
        // to make document() virtual, though I am not sure how we'd go about making IteratorScorer subclasses of
        // relevant_document_provider do the right thing.
        //#define RDP_NEED_TOTAL_MATCHES 1
        struct relevant_document_provider {
                virtual isrc_docid_t document() const noexcept = 0;

#ifdef RDP_NEED_TOTAL_MATCHES
                virtual uint32_t total_matches() = 0;
#endif

                // In the past, relevant_document_provider() had a virtual double score() decl.
                // but that meant we 'd inflate the vtable of Iterators, which are also relevant_document_provider
                // for no reason -- they do not provide scores.
                //
                // Instead, considering that if you invoke score() it must be because the relevant_document_provider you are
                // invoking it on is an IteratorScorer(), we just do what score() impl. does and thereby
                // no longer need to make score() virtual.
                //
                // The only downside is that relevant_document is somewhat ugly now . It contains a dummy iterator which is
                // used just for setting the current document. It's a fair tradeoff though, and consdidering we only
                // make use of it for ExecFlags::DocumentsOnly, it's OK.
                inline double score();
        };

        namespace DocsSetIterators {
                struct Iterator;
        }

        // This simply wraps an iterator, and also provides a score for it when asked.
        struct IteratorScorer
            : public relevant_document_provider {
                DocsSetIterators::Iterator *const it;

                IteratorScorer(DocsSetIterators::Iterator *const it_)
                    : it{it_} {
                }

                virtual ~IteratorScorer() {
                }

#ifdef RDP_NEED_TOTAL_MATCHES
                inline uint32_t total_matches() override final;
#endif

                inline isrc_docid_t document() const noexcept override final;

                // Will return the wrapped iterator, except for e.g Filter and Optional
                // where we only really want to advance the required or main respectively iterator
                // For now, this is not supported, but it may be in the future as an optimization.
                // TODO: consider this
                inline virtual DocsSetIterators::Iterator *iterator() {
                        return it;
                }

                virtual double iterator_score() = 0;
        };

        double relevant_document_provider::score() {
                // If you tried to get a score, then this is a IteratorScorer
                // we used to virtual double score() = 0 here
                // but we really don't want to inflate Iterator's vtable with it
                return static_cast<IteratorScorer *>(this)->iterator_score();
        }
} // namespace Trinity

#ifdef __clang__
#pragma GCC diagnostic pop
#endif
