from services.commands_registry import bind_default_handler, command_specs, validate_registry


async def _dummy_handler(update, context, args):
    _ = (update, context, args)
    return None


def test_registry_validation_passes_once_default_handler_bound():
    bind_default_handler(_dummy_handler)
    issues = validate_registry()
    assert issues == []


def test_registry_has_unique_command_ids():
    specs = command_specs()
    ids = [spec.command_id for spec in specs]
    assert len(ids) == len(set(ids))
