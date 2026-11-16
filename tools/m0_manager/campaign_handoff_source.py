"""Project one terminal campaign handoff from an existing read-only ledger snapshot."""

from dataclasses import asdict
import re
import sqlite3

from dispatch.sa_dispatch.campaign_validation import (
    DEVICES, PREFIX, ROUTE, _CampaignProofs, budget_fields, budget_id, handoff_id as expected_handoff_id,
    schedule_id, schedule_template, slot_end, slot_start, task_fingerprint, task_spec, validate_config,
)
from dispatch.sa_dispatch.canonical import canonical_json
from dispatch.sa_dispatch.models import DispatchError

from .binding import instant, parse_json, token
from .readonly import SourceError


MAX_FIELD = 16 * 1024
MAX_SCALAR = 300
SAFE_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}')
CAMPAIGN_HANDOFF = re.compile(re.escape(PREFIX) + r'([A-Za-z0-9][A-Za-z0-9_.-]{0,47}):slot:([1-9][0-9]?)')
UNKNOWN = 'source_campaign_handoff_unproven'


def _require(condition):
    if not condition:
        raise SourceError(UNKNOWN)


def _find(conn, table, columns, where, params, *, optional=False):
    # All identifiers and predicates are fixed in this module. Bound fields in
    # SQL so a corrupt record cannot expand the read.
    fields = []
    for column in columns.split():
        limit = MAX_FIELD if column.endswith('_json') or column == 'task_template' else MAX_SCALAR
        fields.append(f'CASE WHEN length({column})<={limit} THEN {column} END AS {column}')
    rows = conn.execute(f'SELECT {",".join(fields)} FROM {table} WHERE {where} LIMIT 2', params).fetchall()
    if optional and not rows:
        return None
    _require(len(rows) == 1)
    return dict(rows[0])


def _event(conn, key):
    event = _find(conn, 'control_events', 'event_key kind actor task_id fence at refs_json detail_code',
                  'event_key=?', (key,))
    parse_json(event['refs_json'])
    return event


def _check_event(event, *, kind, actor, at, refs, task_id=None, fence=None):
    _require((event['kind'], event['actor'], event['task_id'], event['fence'], event['at'],
              parse_json(event['refs_json']), event['detail_code']) ==
             (kind, actor, task_id, fence, at, refs, 'observed'))


def _schedule(conn, campaign, role):
    identity = schedule_id({'campaign_id': campaign}, role)
    row = _find(conn, 'schedules', 'id execution_route version interval_seconds max_slots task_template',
                'id=?', (identity,))
    template = parse_json(row['task_template'])
    config = validate_config(template['campaign'])
    _require((row['execution_route'], row['version'], row['interval_seconds'], row['max_slots'],
              config['campaign_id'], template) ==
             (ROUTE, 1, config['interval_seconds'], config['slots'], campaign, schedule_template(config, role)))
    return config


def _fired_task(conn, config, role, slot):
    spec = task_spec(config, role, slot)
    start = slot_start(config, slot)
    firing = _find(conn, 'schedule_firings', 'fire_key task_id version outcome at execution_route task_template',
                   'schedule_id=? AND scheduled_at=?', (schedule_id(config, role), start))
    _require((firing['execution_route'], firing['version'], firing['outcome'], firing['fire_key'],
              firing['task_template']) ==
             (ROUTE, 1, 'created', spec.idem_key, canonical_json(asdict(spec)).decode('utf-8'))
             and start <= firing['at'] < slot_end(config, slot) and SAFE_ID.fullmatch(firing['task_id']))
    task = _find(conn, 'tasks', 'task_id idem_key fingerprint kind kind_version track execution_route '
                 'side_effect access egress max_attempts status attempts fence created_at updated_at result_json',
                 'task_id=?', (firing['task_id'],))
    _require((task['idem_key'], task['fingerprint'], task['kind'], task['kind_version'], task['track'],
              task['execution_route'], task['side_effect'], task['access'], task['egress'], task['max_attempts'],
              task['attempts'], task['fence'], task['created_at']) ==
             (spec.idem_key, task_fingerprint(spec), spec.kind, spec.kind_version, spec.track,
              ROUTE, 'external', spec.access, spec.egress, spec.max_attempts, 1, 1, firing['at']))
    return task


def _task_records(conn, task):
    key = (task['task_id'],)
    records = {}
    for table, columns in (
        ('leases', 'task_id lease_id device_id fence granted_at expires_at state completion_json'),
        ('control_task_rules', 'task_id fingerprint budget_id reserve_units approval_scope approval_id'),
        ('control_approvals', 'task_id approval_id fingerprint scope decision actor evidence_ref expires_at at'),
        ('control_reservations', 'task_id budget_id fingerprint fence reserved_units created_at state actual_units '
                                 'settled_at uncertain_at evidence_ref'),
    ):
        records[table] = [_find(conn, table, columns, 'task_id=?', key)]
    evidence = _find(conn, 'evidence', 'task_id kind fence access ref_json content_hash artifact_hash',
                     'task_id=?', key, optional=True)
    records['evidence'] = [] if evidence is None else [evidence]
    parse_json(task['result_json'])
    parse_json(records['leases'][0]['completion_json'])
    return records


def _project_task(conn, proofs, config, slot, role, task, context_ref):
    _require(proofs.validate_task(config, role, slot, task, context_ref) == (1, 0))
    tid = task['task_id']
    lease, = proofs.task_related['leases'][tid]
    approval, = proofs.task_related['control_approvals'][tid]
    reservation, = proofs.task_related['control_reservations'][tid]
    _require(SAFE_ID.fullmatch(lease['lease_id']))
    _check_event(_event(conn, f'settled:{tid}:1'), kind='budget.settled', actor=DEVICES[role],
                 task_id=tid, fence=1, at=task['updated_at'], refs=[reservation['evidence_ref']])
    return {'role': role, 'device_id': DEVICES[role], 'task_id': tid, 'state': task['status'],
            'fingerprint': task['fingerprint'], 'fence': 1, 'attempts': 1, 'lease_id': lease['lease_id'],
            'approval_ref': approval['evidence_ref'], 'output_ref': parse_json(task['result_json']).get('result_ref'),
            'reservation': {'state': 'settled', 'budget_id': reservation['budget_id'],
                            'actual_units': 1, 'usage_ref': reservation['evidence_ref']}}


def _read(conn, source_id, identity, campaign, slot, observed_at):
    handoff = _find(conn, 'control_handoffs', 'handoff_id from_conv to_conv project_id scope scope_refs_json '
                    'authorized_by authorization_ref expires_at state receipt_ref evidence_refs_json created_at updated_at',
                    'handoff_id=?', (identity,), optional=True)
    if handoff is None:
        return None
    state = handoff['state']
    _require(state in ('completed', 'rejected') and
             (handoff['from_conv'], handoff['to_conv']) == tuple(DEVICES.values()) and
             instant(handoff['updated_at']) <= instant(observed_at))
    config = _schedule(conn, campaign, 'producer')
    _require(config == _schedule(conn, campaign, 'reviewer') and slot <= config['slots']
             and identity == expected_handoff_id(config, slot))
    producer, reviewer = (_fired_task(conn, config, role, slot) for role in DEVICES)
    _require((producer['status'], reviewer['status']) == ('succeeded', 'succeeded' if state == 'completed' else 'failed'))
    task_records = _task_records(conn, producer)
    for table, rows in _task_records(conn, reviewer).items():
        task_records[table].extend(rows)
    context_event = _event(conn, 'campaign-context:' + identity)
    proofs = _CampaignProofs({**task_records, 'control_handoffs': [handoff], 'control_events': [context_event]}, None)
    context_ref = proofs.validate_handoff(config, slot, producer, reviewer)
    accept_ref, _, _ = parse_json(context_event['refs_json'])
    for transition, actor, at, ref in (
        ('offered', DEVICES['producer'], handoff['created_at'], handoff['authorization_ref']),
        ('accepted', DEVICES['reviewer'], context_event['at'], accept_ref),
        (state, DEVICES['reviewer'], handoff['updated_at'], handoff['receipt_ref']),
    ):
        _check_event(_event(conn, f'handoff:{identity}:{transition}'), kind=f'handoff.{transition}',
                     actor=actor, at=at, refs=[ref])
    _require(context_event['detail_code'] == 'observed' and instant(producer['created_at']) <=
             instant(handoff['created_at']) <= instant(reviewer['created_at']) <= instant(handoff['updated_at']))
    used = _find(conn, 'control_budgets', 'budget_id period unit limit_units reserved_units used_units starts_at ends_at',
                 'budget_id=?', (budget_id(config),))
    _require(all(used[key] == value for key, value in budget_fields(config).items()) and
             type(used['used_units']) is int and type(used['reserved_units']) is int and
             used['used_units'] >= 2 and used['reserved_units'] >= 0 and
             used['used_units'] + used['reserved_units'] <= used['limit_units'])
    return {
        'kind': 'handoff.campaign_observed', 'source_id': source_id, 'handoff_id': identity,
        'campaign_id': campaign, 'slot': slot, 'state': state, 'as_of': observed_at,
        'created_at': handoff['created_at'], 'updated_at': handoff['updated_at'],
        'context_read_at': context_event['at'], 'expires_at': handoff['expires_at'],
        'state_basis': 'historical', 'coverage': 'partial', 'unknown': ['cas_content_integrity'],
        'proof_basis': 'ledger_hash_linkage', 'content_integrity': 'not_checked',
        'offer_ref': handoff['authorization_ref'], 'context_ref': context_ref,
        'accepted_ref': accept_ref, 'terminal_receipt_ref': handoff['receipt_ref'],
        'producer': _project_task(conn, proofs, config, slot, 'producer', producer, None),
        'reviewer': _project_task(conn, proofs, config, slot, 'reviewer', reviewer, context_ref),
        'budget': {'budget_id': used['budget_id'], 'unit': used['unit'], 'limit_units': used['limit_units'],
                   'used_units': used['used_units'], 'held_units': used['reserved_units']},
    }


def read_handoff(conn, *, source_id, handoff_id, observed_at):
    """Return one bounded terminal chain without opening CAS or modifying the source."""
    if not token(source_id) or not isinstance(handoff_id, str) or not SAFE_ID.fullmatch(handoff_id):
        raise SourceError('source_invalid_identity')
    match = CAMPAIGN_HANDOFF.fullmatch(handoff_id)
    if match is None:
        return None
    try:
        _require(isinstance(observed_at, str) and len(observed_at) <= 40)
        instant(observed_at)
        return _read(conn, source_id, handoff_id, match[1], int(match[2]), observed_at)
    except (DispatchError, sqlite3.Error, AttributeError, KeyError, OverflowError, TypeError, ValueError,
            RecursionError) as error:
        raise SourceError(UNKNOWN) from error
