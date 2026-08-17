import pytest

from common.conf.domain_config import DomainConfig, DomainConfigError


def domain_payload():
    return {
        "version": 3,
        "topics": {
            "Software Development": {
                "description": "Software work",
                "active": True,
            },
            "Archive": {"active": False},
        },
        "entity_types": {
            "Project": {
                "topic": "software development",
                "description": "An organized software effort.",
                "labels": ["Software Project", "application"],
            },
            "Technology": {
                "topic": "Software Development",
                "description": "A language or development tool.",
                "labels": ["programming language", "framework"],
            },
            "Archived Item": {
                "topic": "Archive",
                "labels": ["old item"],
            },
        },
        "relationships": {
            "USES": {
                "source_types": ["project"],
                "target_types": ["technology"],
            },
        },
    }


@pytest.mark.unit
@pytest.mark.no_network
def test_domain_config_compiles_canonical_lookups():
    config = DomainConfig.from_mapping(domain_payload())
    compiled = config.compile()

    assert config.version == 3
    assert config.entity_types[0].topic == "Software Development"
    assert config.entity_types[0].labels == ("software project", "application")
    assert compiled.resolve_entity_type(" SOFTWARE PROJECT ") == "Project"
    assert compiled.topic_for_entity_type("project") == "Software Development"
    assert compiled.normalize_topic("software development") == "Software Development"
    assert compiled.active_topics == ("Software Development",)
    assert compiled.active_entity_types == ("Project", "Technology")
    assert compiled.labels == (
        "application",
        "framework",
        "programming language",
        "software project",
    )
    assert compiled.relationship("uses").source_types == ("Project",)
    assert compiled.relationship_allows("USES", "Project", "Technology")
    assert not compiled.relationship_allows("USES", "Technology", "Project")
    assert compiled.topic_descriptions["Software Development"] == "Software work"
    assert compiled.entity_descriptions["Project"] == "An organized software effort."
    assert "USES: Project -> Technology" in compiled.relationship_block
    assert type(compiled).from_dict(compiled.to_dict()) == compiled


@pytest.mark.unit
@pytest.mark.no_network
def test_inactive_topic_disables_its_entity_type_and_labels():
    compiled = DomainConfig.from_mapping(domain_payload()).compile()

    assert compiled.resolve_entity_type("old item") is None
    assert compiled.topic_for_entity_type("Archived Item") is None
    assert not compiled.is_active_entity_type("archived item")
    assert "old item" not in compiled.label_block


@pytest.mark.unit
@pytest.mark.no_network
def test_compiled_lookup_tables_are_immutable():
    compiled = DomainConfig.from_mapping(domain_payload()).compile()

    with pytest.raises(TypeError):
        compiled.label_to_entity_type["new label"] = "Project"
    with pytest.raises(TypeError):
        compiled.relationships["NEW_RELATIONSHIP"] = compiled.relationship("USES")


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("payload_update", "message"),
    [
        ({"unexpected": True}, "Unknown domain configuration fields"),
        ({"topics": {"Software Development": {"unknown": True}}}, "Unknown fields"),
    ],
)
def test_domain_config_rejects_unknown_fields(payload_update, message):
    payload = domain_payload()
    payload.update(payload_update)

    with pytest.raises(DomainConfigError, match=message):
        DomainConfig.from_mapping(payload)


@pytest.mark.unit
@pytest.mark.no_network
def test_domain_config_rejects_ambiguous_labels_and_invalid_relationships():
    duplicate_label = domain_payload()
    duplicate_label["entity_types"]["Technology"]["labels"] = ["application"]
    with pytest.raises(DomainConfigError, match="claimed by both"):
        DomainConfig.from_mapping(duplicate_label)

    unknown_endpoint = domain_payload()
    unknown_endpoint["relationships"]["USES"]["target_types"] = ["Platform"]
    with pytest.raises(DomainConfigError, match="unknown entity types"):
        DomainConfig.from_mapping(unknown_endpoint)

    asymmetric_symmetric = domain_payload()
    asymmetric_symmetric["relationships"]["COLLABORATES_WITH"] = {
        "source_types": ["Project"],
        "target_types": ["Technology"],
        "symmetric": True,
    }
    with pytest.raises(DomainConfigError, match="same source and target"):
        DomainConfig.from_mapping(asymmetric_symmetric)


@pytest.mark.unit
@pytest.mark.no_network
def test_domain_config_round_trips_through_mapping():
    config = DomainConfig.from_mapping(domain_payload())

    assert DomainConfig.from_mapping(config.to_dict()) == config
