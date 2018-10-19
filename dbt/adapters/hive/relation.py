from dbt.adapters.default import DefaultRelation


class HiveRelation(DefaultRelation):

    DEFAULTS = {
        'metadata': {
            'type': 'HiveRelation'
        },
        'quote_character': '`',
        'quote_policy': {
            'schema': True,
            'identifier': True
        },
        'include_policy': {
            'schema': True,
            'identifier': True
        }
    }

    PATH_SCHEMA = {
        'type': 'object',
        'properties': {
            'schema': {'type': ['string', 'null']},
            'identifier': {'type': 'string'},
        },
        'required': ['schema', 'identifier'],
    }

    POLICY_SCHEMA = {
        'type': 'object',
        'properties': {
            'schema': {'type': 'boolean'},
            'identifier': {'type': 'boolean'},
        },
        'required': ['schema', 'identifier'],
    }

    SCHEMA = {
        'type': 'object',
        'properties': {
            'metadata': {
                'type': 'object',
                'properties': {
                    'type': {
                        'type': 'string',
                        'const': 'HiveRelation',
                    },
                },
            },
            'type': {
                'enum': DefaultRelation.RelationTypes + [None],
            },
            'path': PATH_SCHEMA,
            'include_policy': POLICY_SCHEMA,
            'quote_policy': POLICY_SCHEMA,
            'quote_character': {'type': 'string'},
        },
        'required': ['metadata', 'type', 'path', 'include_policy',
                     'quote_policy', 'quote_character']
    }

    PATH_ELEMENTS = ['schema', 'identifier']

    @classmethod
    def _create_from_node(cls, config, node, **kwargs):
        return cls.create(
            schema=node.get('schema'),
            identifier=node.get('alias'),
            **kwargs)

    @classmethod
    def create(cls, schema=None,
               identifier=None, table_name=None,
               type=None, **kwargs):
        if table_name is None:
            table_name = identifier

        return cls(type=type,
                   path={
                       'schema': schema,
                       'identifier': identifier
                   },
                   table_name=table_name,
                   **kwargs)