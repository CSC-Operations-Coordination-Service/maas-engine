"""Checks of engine configurations"""

from maas_engine.config.validation import (
    engine_ids,
    import_problems,
    routing_keys,
    set_problems,
    structure_problems,
)

QUERY = {
    "amqp": [
        {"name": "collect-exchange", "queues": []},
        {
            "name": "etl-exchange",
            "queues": [
                {
                    "name": "etl-query.cds",
                    "routing_key": "query.cds",
                    "events": [{"id": "QUERY_ENGINE", "send_reports": True}],
                }
            ],
        },
    ]
}

DEFAULTS = {
    "modules": ["maas_engine.engine.query"],
    "model": "maas_model",
    "defaults": [{"id": "QUERY_ENGINE", "chunk_size": 10}],
    "logging": {"shapely.geos": "CRITICAL"},
}


def test_well_formed_configurations():
    assert not structure_problems(QUERY)
    assert not structure_problems(DEFAULTS)
    assert not structure_problems({})


def test_structure_problems_name_what_is_wrong():
    problems = structure_problems(
        {
            "amqp": [
                {"name": "etl-exchange", "queues": [{"name": "q", "events": [1]}]},
                {"queues": []},
            ],
            "modules": "maas_cds",
            "model": "",
            "defaults": [{"chunk_size": 1}],
            "logging": {"x": "LOUD"},
            "engines": [],
        }
    )

    assert problems == [
        "unknown key 'engines'"
        " (expected one of amqp, modules, model, defaults, logging)",
        "exchange etl-exchange, queue q: routing_key must be a non-empty string",
        "exchange etl-exchange, queue q: an event must be an engine id or an object"
        " with an id",
        "amqp[1] must be an object with a name",
        "modules must be a list of module names",
        "model must be a module name",
        "defaults must be a list of objects with an id",
        "logging level of 'x' is not a level: 'LOUD'",
    ]


def test_not_an_object():
    assert structure_problems([]) == ["the configuration is not a JSON object"]


def test_engine_ids_and_routing_keys():
    content = {
        "amqp": [
            {
                "name": "etl-exchange",
                "queues": [
                    {"name": "a", "routing_key": "new.b", "events": ["B", {"id": "A"}]},
                    {"name": "b", "routing_key": "new.a", "events": ["B"]},
                ],
            }
        ]
    }

    assert engine_ids(content) == ["A", "B"]
    assert routing_keys(content) == ["new.a", "new.b"]
    assert not engine_ids(DEFAULTS) and not routing_keys(DEFAULTS)


def test_set_problems_finds_duplicate_queues_and_unknown_engines():
    sources = [
        ("default/default.json", DEFAULTS),
        ("specific/cds-query.json", QUERY),
        ("specific/copy.json", QUERY),
        ("specific/broken.json", {"amqp": 1}),
    ]

    assert set_problems(sources, known_engine_ids={"OTHER"}) == [
        "specific/broken.json: amqp must be a list of exchanges",
        "specific/copy.json: queue etl-query.cds of etl-exchange is declared by"
        " specific/cds-query.json too",
        "specific/cds-query.json: unknown engine ids QUERY_ENGINE",
        "specific/copy.json: unknown engine ids QUERY_ENGINE",
    ]
    assert not set_problems(sources[:2], known_engine_ids={"QUERY_ENGINE"})
    assert not set_problems(sources[:2])


def test_import_problems():
    assert not import_problems([("default/default.json", DEFAULTS)])

    problems = import_problems(
        [("default/modules.json", {"modules": ["maas_engine.nothing_here"]})]
    )

    assert len(problems) == 1
    assert problems[0].startswith(
        "default/modules.json: cannot import maas_engine.nothing_here:"
    )
