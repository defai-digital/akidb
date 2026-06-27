//! Retrieval-quality evaluation harness (recall@k, nDCG@k).
//!
//! Compares dense-only, lexical-only, and hybrid (RRF) retrieval on a controlled
//! corpus where relevant documents are split between semantic and lexical
//! matches. Run:
//!
//!     cargo run -p akidb-retrieval --example quality_eval
//!
//! Exits non-zero if hybrid does not beat both single-mode retrievers, so it can
//! act as a CI quality gate.

use akidb_retrieval::run_controlled_eval;

fn main() {
    let k = 10;
    let summary = run_controlled_eval(/*queries*/ 50, /*distractors*/ 2000, /*dims*/ 64, k);

    println!("Retrieval quality — controlled corpus");
    println!(
        "  queries={}  k={}  (each query: 5 semantic + 5 lexical relevant docs, 2000 distractors)\n",
        summary.queries, summary.k
    );
    println!("  strategy   recall@{k}   nDCG@{k}", k = k);
    println!("  --------   ---------   -------");
    println!("  dense      {:>8.3}   {:>6.3}", summary.dense.recall, summary.dense.ndcg);
    println!("  lexical    {:>8.3}   {:>6.3}", summary.lexical.recall, summary.lexical.ndcg);
    println!("  hybrid     {:>8.3}   {:>6.3}", summary.hybrid.recall, summary.hybrid.ndcg);

    let lift_dense = summary.hybrid.recall - summary.dense.recall;
    let lift_lex = summary.hybrid.recall - summary.lexical.recall;
    println!(
        "\n  hybrid recall lift: +{:.3} vs dense, +{:.3} vs lexical",
        lift_dense, lift_lex
    );

    if summary.hybrid.recall <= summary.dense.recall || summary.hybrid.recall <= summary.lexical.recall {
        eprintln!("QUALITY GATE FAILED: hybrid did not beat both single-mode retrievers");
        std::process::exit(1);
    }
    println!("\n  QUALITY GATE PASSED");
}
