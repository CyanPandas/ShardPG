"""Read-only Git comparison for the pinned new1 step-3 inventory."""
import collections
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
OUT = Path(__file__).resolve().parent
FA23 = '44a3e2d83a20bf7d32d8004a218dd7c5591bb602'
MMP = 'bbee3e1cb7f9e0e49b7a246153fce078c173bee0'
TEST = 'b149622efddb29032f2932cc6f5e7b2e2de642c8'
SPLIT = '01e5635a2ca6010c929130ed26281d34838b8686'
SCOPE = ['pg-partdist-src', 'pg-raft-src', 'tpcc']


def git(*args):
    return subprocess.check_output(['git', '-C', str(ROOT), *args], text=True).strip()


def tree(ref):
    result = {}
    for row in git('ls-tree', '-r', ref, '--', *SCOPE).splitlines():
        meta, path = row.split('\t', 1)
        mode, kind, oid = meta.split()
        result[path] = {'mode': mode, 'type': kind, 'oid': oid}
    return result


def compare(a, b, paths):
    result = []
    for path in paths:
        left, right = a.get(path), b.get(path)
        status = ('absent_both' if left is None and right is None else
                  'absent_test' if left is None else
                  'absent_mmp' if right is None else
                  'identical' if left == right else 'different')
        result.append({'path': path, 'status': status,
                       'test': left, 'mmp': right})
    return result


if __name__ == '__main__':
    test, mmp = tree(TEST), tree(MMP)
    paths = git('diff', '--name-only', SPLIT, TEST, '--', *SCOPE).splitlines()
    files = compare(test, mmp, paths)
    commits = []
    for commit in git('rev-list', '--reverse', SPLIT + '..' + TEST).splitlines():
        touched = git('diff-tree', '--no-commit-id', '--name-only', '-r', commit,
                      '--', *SCOPE).splitlines()
        rows = compare(test, mmp, touched)
        commits.append({'commit': commit, 'subject': git('show', '-s', '--format=%s', commit),
                        'final_tree_file_counts': dict(collections.Counter(r['status'] for r in rows)),
                        'files': rows})
    evidence = {
        'fa23': FA23, 'mmp': MMP, 'test': TEST, 'test_mmp_base': SPLIT,
        'fa23_mmp_base': git('merge-base', FA23, MMP),
        'scope': SCOPE,
        'method': 'Compare final test and mmp blobs for paths changed by the 20 test commits. Identical files do not prove all earlier behavior survived; different files do not prove a fix is missing. No runtime tests performed.',
        'file_counts': dict(collections.Counter(r['status'] for r in files)),
        'files': files, 'commits': commits,
        'mmp_changes_after_test': git('diff', '--name-status', TEST, MMP, '--', *SCOPE).splitlines(),
        'fa23_to_mmp_changes': git('diff', '--name-status', FA23, MMP, '--', *SCOPE).splitlines(),
        'build_layout': {ref: git('ls-tree', ref, 'postgres-src', 'citus-src', 'pg-install').splitlines()
                         for ref in [FA23, MMP]},
    }
    assert len(commits) == 20
    assert evidence['file_counts'] == {'identical': 45, 'different': 19}
    (OUT / 'SOURCE_EVIDENCE.json').write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
    print('20 commits; 64 paths: 45 identical, 19 different, none missing')
