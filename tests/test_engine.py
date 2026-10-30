from collections import namedtuple
import math

import maas_model

from maas_engine.engine.base import EngineReport
from maas_engine.engine.rawdata import RawDataEngine


def test_split_report():
    # cover odd & even
    for data_length in (1, 32):
        for chunk_size in range(1, data_length):
            report = EngineReport(
                "action", list(range(data_length)), "SomeClass", chunk_size=chunk_size
            )

            splitted_reports = list(EngineReport.split_reports([report]))

            assert len(splitted_reports) == math.ceil(len(report.data_ids) / chunk_size)

            for splitted_report in splitted_reports:
                assert len(splitted_report.data_ids) <= chunk_size


def test_initial_state_maas_documents():
    Args = namedtuple("Args", ["force"])
    args = Args(True)

    doc1 = maas_model.MAASDocument()
    doc1.meta.id = "a"
    doc2 = maas_model.MAASDocument()
    doc2.meta.id = "b"

    rde = RawDataEngine(args)

    rde.populate_initial_state([doc1, doc2])
    assert rde.initial_state_dict == {}

    rde.consolidated_documents = [doc1, doc2]

    doc1.attr_a = "AA"

    rde.populate_output_cache()

    assert rde._output_cache == {"a": doc1, "b": doc2}

    args = Args(False)
    rde = RawDataEngine(args)

    rde.populate_initial_state([doc1, doc2])
    assert rde.initial_state_dict == {"a": {"attr_a": "AA"}, "b": {}}

    rde.consolidated_documents = [doc1, doc2]

    doc1.attr_a = "BB"

    rde.populate_output_cache()
    assert rde._output_cache == {"a": doc1}


def test_initial_state_other_type():
    """

    Check if populate_initial_state handle non maas-document entities
    """
    rde = RawDataEngine()
    rde.populate_initial_state([(23, 1), (27, 2)])
    assert rde.initial_state_dict == {}


def test_split_report_keeps_exchange_and_chunk_size():
    report = EngineReport(
        "new.raw.data.prip-product",
        ["1", "2", "3"],
        document_class="PripProduct",
        chunk_size=2,
        exchange="collect-exchange",
    )
    chunks = list(EngineReport.split_reports([report]))
    assert [c.exchange for c in chunks] == ["collect-exchange", "collect-exchange"]
    assert [c.chunk_size for c in chunks] == [2, 2]


def test_merge_reports_keeps_exchange():
    a = EngineReport("k", ["1"], exchange="collect-exchange")
    b = EngineReport("k", ["2"], exchange="collect-exchange")
    assert EngineReport.merge_reports([a, b])[0].exchange == "collect-exchange"
