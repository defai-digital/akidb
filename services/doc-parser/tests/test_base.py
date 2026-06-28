"""Tests for shared parser helpers."""

from parser.parsers.base import table_rows_to_retrieval_text


def test_table_rows_to_retrieval_text_handles_sparse_rows():
    text = table_rows_to_retrieval_text(
        ["customer", "", "contract_amount"],
        [
            ["HGC", "2025", "1200"],
            ["", "2026", ""],
            ["DEF", "", "3400"],
        ],
    )

    assert text.splitlines() == [
        "customer contract_amount",
        "customer HGC 2025 contract_amount 1200",
        "2026",
        "customer DEF contract_amount 3400",
    ]
