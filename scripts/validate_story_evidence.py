"""Validate the approved, versioned story data without fetching primary documents."""
from __future__ import annotations

import argparse
import json
import re
from decimal import Decimal, localcontext
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = {'id', 'metric', 'value', 'unit', 'reporting_period', 'as_of', 'entity',
            'perimeter', 'source_url', 'document_page', 'audit_status', 'definition',
            'calculation_input_ids', 'uncertainty', 'value_status', 'research_as_of'}
MISSING = {'MISSING-AI_paid_capex', 'MISSING-AI_ROI', 'MISSING-dashcam_installed_km',
           'MISSING-national_utilisation', 'MISSING-NHAI_FY25_classic_comparable'}


def validate(observations: dict, calculations: dict, sources: dict) -> dict:
    for document in (observations, calculations, sources):
        if document.get('schema_version') != '1.0.0':
            raise ValueError('Unsupported story schema')
    rows, recipes, references = observations['observations'], calculations['calculations'], sources['sources']
    if observations.get('research_as_of') != '2026-10-03':
        raise ValueError('Unexpected research cutoff')
    if (len(rows), len(recipes), len(references)) != (548, 55, 28):
        raise ValueError('Unexpected version 1 evidence counts')
    by_id, by_url, recipes_by_id = {}, {}, {}
    for source in references:
        url = urlsplit(source['url'])
        if url.scheme != 'https' or not url.hostname or url.username or url.password:
            raise ValueError('Invalid primary source URL')
        if source['url'] in by_url:
            raise ValueError('Duplicate primary source URL')
        by_url[source['url']] = source
    for row in rows:
        if not REQUIRED <= row.keys() or not re.fullmatch(r'[A-Za-z0-9_-]+', row['id']):
            raise ValueError('Incomplete observation')
        for field in REQUIRED - {'value', 'calculation_input_ids'}:
            if not isinstance(row[field], str) or not row[field].strip():
                raise ValueError('Empty observation context')
        if row['research_as_of'] != observations['research_as_of']:
            raise ValueError('Observation research cutoff differs')
        if row['id'] in by_id:
            raise ValueError('Duplicate observation ID')
        if row['value_status'] not in {'reported', 'reported_estimate', 'derived', 'unavailable'}:
            raise ValueError('Unknown observation status')
        if (row['value'] is None) != (row['value_status'] == 'unavailable'):
            raise ValueError('Unavailable observations must remain null')
        if row['value'] is not None and (not isinstance(row['value'], str) or not re.fullmatch(r'-?\d+(?:\.\d+)?', row['value'])):
            raise ValueError('Observation precision must be a finite decimal string')
        source = by_url.get(row['source_url'])
        if not source or row['id'] not in source['observation_ids'] or row['entity'] not in source['entities'] or row['document_page'] not in source['page_references']:
            raise ValueError('Observation source index does not match URL/entity/page')
        by_id[row['id']] = row
    membership = [item for source in references for item in source['observation_ids']]
    if len(membership) != len(rows) or set(membership) != set(by_id):
        raise ValueError('Source index must cover every observation exactly once')
    for recipe in recipes:
        if recipe['id'] in recipes_by_id:
            raise ValueError('Duplicate calculation ID')
        recipes_by_id[recipe['id']] = recipe
    if set(recipes_by_id) != {row['id'] for row in rows if row['value_status'] == 'derived'}:
        raise ValueError('Derived observations and calculation notes differ')
    visited, active = set(), set()

    def reproduce(identifier: str) -> Decimal:
        row = by_id[identifier]
        if row['value'] is None:
            raise ValueError('Calculation input is unavailable')
        if identifier not in recipes_by_id:
            return Decimal(row['value'])
        if identifier in active:
            raise ValueError('Calculation dependency cycle')
        if identifier in visited:
            return Decimal(row['value'])
        active.add(identifier)
        recipe = recipes_by_id[identifier]
        if recipe['input_ids'] != row['calculation_input_ids'] or recipe['unit'] != row['unit']:
            raise ValueError('Calculation lineage or output unit differs')
        values = [reproduce(item) for item in recipe['input_ids']]
        if not values or len({by_id[item]['unit'] for item in recipe['input_ids']}) != 1:
            raise ValueError('Calculation inputs must share a unit')
        operation = recipe['operation']
        with localcontext() as context:
            context.prec = 28
            if operation == 'sum':
                result = sum(values, Decimal(0))
            elif operation == 'difference':
                result = values[0] - sum(values[1:], Decimal(0))
            elif operation in {'percent', 'ratio', 'growth_percent'}:
                if len(values) != 2 or values[1] == 0:
                    raise ValueError('Invalid calculation denominator')
                result = values[0] / values[1]
                if operation == 'growth_percent':
                    result = (result - 1) * 100
                elif operation == 'percent':
                    result *= 100
            else:
                raise ValueError('Unsupported calculation operation')
        if result != Decimal(recipe['expected_decimal']) or result != Decimal(row['value']):
            raise ValueError('Calculation does not reproduce: ' + identifier)
        active.remove(identifier)
        visited.add(identifier)
        return result

    for identifier in recipes_by_id:
        reproduce(identifier)
    missing = [row['id'] for row in rows if row['value'] is None]
    if set(missing) != MISSING:
        raise ValueError('Unexpected unavailable observations')
    return {'schema_version': '1.0.0', 'observations': len(rows), 'calculations_reproduced': len(visited),
            'primary_sources': len(references), 'unavailable_observations': missing,
            'arithmetic': 'Decimal precision 28; growth=(current/prior-1)*100',
            'scope': 'Internal consistency and arithmetic; primary documents are not fetched by this validator'}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT / 'data/research/nhai-nhit')
    args = parser.parse_args()
    documents = [json.loads((args.root / name).read_text()) for name in
                 ('observations.v1.json', 'calculations.v1.json', 'sources.v1.json')]
    print(json.dumps(validate(*documents), indent=2))


if __name__ == '__main__':
    main()
