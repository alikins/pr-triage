#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2014 Matt Martz
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import os
import re
import yaml
import logging
import jinja2
import cPickle

import pprint
pp = pprint.pprint

from github import Github
from datetime import datetime
from collections import defaultdict, OrderedDict

import botmetadata

try:
    import pyrax
    HAS_PYRAX = True
except ImportError:
    HAS_PYRAX = False

log = logging.getLogger('pr-triage')
log_format = '%(asctime)s %(levelname)s %(name)s %(funcName)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
#logging.basicConfig(level=logging.DEBUG, format=log_format)


def get_config():
    config_files = [
        './triage.yaml',
        os.path.expanduser('~/.triage.yaml'),
        '/etc/triage.yaml'
    ]
    for config_file in config_files:
        try:
            with open(os.path.realpath(config_file)) as f:
                config = yaml.load(f)
        except:
            pass
        else:
            return config

    raise SystemExit('Config file not found at: %s' % ', '.join(config_files))

def repo_config(config):
    if not isinstance(config['github_repository'], list):
        repos = [config['github_repository']]
    else:
        repos = config['github_repository']
    return repos

def get_data_path(config):
    repos = repo_config(config)
    data_dir_path = config.get('data_path', 'data/')
    data_file_name = '-'.join(repos).replace('/','_') + '.pickle'
    if not os.path.exists(data_dir_path):
        log.warning('The data_path \"%s\" did not exist. Creating it now.', data_dir_path)
        os.makedirs(data_dir_path)
    data_path = os.path.join(data_dir_path, data_file_name)
    return data_path


def read_maintainers_old_():
    path_to_maintainers = {}
    maintainer_to_paths = defaultdict(list)

    with open('MAINTAINERS.txt', 'r') as f:
        for line in f:
            owner_space = (line.split(': ')[0]).strip()
            maintainers_string = (line.split(': ')[-1]).strip()
            maintainer_list = maintainers_string.split(' ')
            path_to_maintainers[owner_space] = maintainer_list

            for maintainer in maintainer_list:
                maintainer_to_paths[maintainer].append(owner_space)

    return maintainer_to_paths, path_to_maintainers


def read_maintainers():
    path_to_maintainers = {}
    maintainer_to_paths = defaultdict(list)

    path_to_labels = {}
    label_to_paths = defaultdict(list)

    path_to_keywords = {}
    keyword_to_paths = defaultdict(list)

    path_to_support_levels = {}
    support_level_to_paths = defaultdict(list)

    bot_meta_path = '/home/adrian/src/ansible/.github/BOTMETA.yml'
    with open(bot_meta_path, 'r') as f:
        bot_meta_contents = f.read()
        botmeta = botmetadata.BotMetadataParser.parse_yaml(bot_meta_contents)

    files_data = botmeta['files']
    for file_name in files_data:
        # print('file_name: %s' % file_name)
        file_data = files_data[file_name]
        # special case no maintainers?
        maintainers = file_data.get('maintainers', [])
        path_to_maintainers[file_name] = maintainers

        for maintainer in maintainers:
            maintainer_to_paths[maintainer].append(file_name)

        labels = file_data.get('labels', [])
        path_to_labels[file_name] = labels
        for label in labels:
            label_to_paths[label].append(file_name)

        keywords = file_data.get('keywords', [])
        path_to_keywords[file_name] = keywords
        for keyword in keywords:
            keyword_to_paths[keyword].append(file_name)

        support_levels = file_data.get('support', [])
        path_to_support_levels[file_name] = support_levels
        for support_level in support_levels:
            support_level_to_paths[support_level].append(file_name)

        # for key in file_data:
        #    if key not in ('maintainers', 'labels', 'keywords', 'maintainers_keys', 'support'):
        #        print('%s: unknown key "%s"' % (file_name, key))

    # to do, repr wrapper so we dont pformat until when and if we log
    pf = pprint.pformat
    log.debug('maintainer_to_paths: %s', pf(dict(maintainer_to_paths)))
    log.debug('path_to_maintainers: %s', pf(path_to_maintainers))
    log.debug('path_to_labels: %s', pf(path_to_labels))
    log.debug('path_to_keyswords: %s', pf(path_to_keywords))

    return {'maintainer': maintainer_to_paths,
            'label': label_to_paths,
            'keyword': keyword_to_paths,
            'support_level': support_level_to_paths}


def scan_issues(config, cached_data=None):
    merge_commit = re.compile("Merge branch \S+ into ", flags=re.I)

    files = defaultdict(list)
    dirs = defaultdict(set)
    users = defaultdict(list)
    conflicts = defaultdict(list)
    ci_failures = defaultdict(list)
    merges = defaultdict(list)
    multi_author = defaultdict(list)
    labels = defaultdict(list)

    prs = {}

    g = Github(client_id=config['github_client_id'],
               client_secret=config['github_client_secret'],
               per_page=100)

    repos = repo_config(config)

    for repo_name in repos:
        log.info('Scanning repo: %s', repo_name)
        repo = g.get_repo(repo_name)

        pull_counter = 0
        # FIXME: build a struct/dict of the data for the new pr, snapshot it to a dir
        #        then add it to the main dicts (files/dirs/authors etc) to avoid re-snapshotting whole list
        for pull in repo.get_pulls():
            pull_counter += 1
            log.info('pull.url: %s (%s of N)', pull.url, pull_counter)
            log.info('pull.id: %s (%s of N)', pull.id, pull_counter)

            # FIXME: make cached_data into dict
            if cached_data and cached_data[7] and pull.url in cached_data[7]:
                log.info('pull.url %s was in cached, updating anyway', pull.url)
            if pull.user is None:
                login = pull.head.user.login
            else:
                login = pull.user.login

            users[login].append(pull)

            if pull.mergeable is False or pull.mergeable_state == 'dirty':
                conflicts[login].append(pull)

            if pull.mergeable_state == 'unstable':
                ci_failures[login].append(pull)

            for pull_file in pull.get_files():
                files[pull_file.filename].append(pull)
                dirs[os.path.dirname(pull_file.filename)].add(pull)

            authors = set()
            for commit in pull.get_commits():
                authors.add(commit.commit.author.email)
                try:
                    if merge_commit.match(commit.commit.message):
                        merges[login].append(pull)
                        break
                except TypeError:
                    pass

            if len(authors) > 1:
                multi_author[login].append(pull)

            prs[pull.url] = pull

            if pull_counter % 100:
                # FIXME: split this into small snapshots in a dir, so we dont have to write out the whole data every time?
                log.info('Saving data snapshot')
                log.info('items - config: %s, files: %s, merges: %s, conflicts: %s, multi_author: %s, ci_failures: %s, prs: %s, dirs: %s, labels: %s',
                         len(config), len(files), len(merges), len(conflicts), len(multi_author), len(ci_failures), len(prs), len(dirs), len(labels))

                snapshot = [config, files, merges, conflicts, multi_author, ci_failures, prs, dirs, labels]
                with open('data/snapshot.pickle', 'w') as f:
                    cPickle.dump(snapshot, f)

    usersbypulls = OrderedDict()
    for user, pulls in sorted(users.items(),
                              key=lambda t: len(t[-1]), reverse=True):
        usersbypulls[user] = pulls

    a = [config, files, usersbypulls, merges, conflicts, multi_author, ci_failures, prs, dirs, labels]

    data_file_name = get_data_path(config)
    log.info('saving data to %s', data_file_name)

    with open(data_file_name, 'w') as f:
        cPickle.dump(a, f)

    return [config, files, usersbypulls, merges, conflicts, multi_author,
            ci_failures, prs, dirs, labels]


def write_html(config, files, users, merges, conflicts, multi_author,
               ci_failures, prs, dirs, maintainers, labels, keywords, support_levels):
    log.info('About to write_html')

    if config.get('use_rackspace', False):
        if not HAS_PYRAX:
            raise SystemExit('The pyrax python module is required to use '
                             'Rackspace CloudFiles')
        pyrax.set_setting('identity_type', 'rackspace')
        credentials = os.path.expanduser(config['pyrax_credentials'])
        pyrax.set_credential_file(credentials, region=config['pyrax_region'])
        cf = pyrax.cloudfiles
        cont = cf.get_container(config['pyrax_container'])

    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    # TODO: make template/htmlout configurable
    loader = jinja2.FileSystemLoader('templates')
    environment = jinja2.Environment(loader=loader, trim_blocks=True)

    if not os.path.isdir('htmlout'):
        os.makedirs('htmlout')

    templates = ['index', 'byfile', 'bydir', 'byuser',
                 'bymergecommits', 'bymaintainer',
                 'byconflict', 'bymultiauthor', 'bycifailures',
                 'bylabel', 'bykeyword', 'bysupportlevel']

    for tmplfile in templates:
        now = datetime.utcnow()
        classes = {}
        for t in templates:
            classes['%s_classes' % t] = 'active' if tmplfile == t else ''

        template = environment.get_template('%s.html' % tmplfile)
        rendered = template.render(files=files, dirs=dirs, users=users,
                                   merges=merges, conflicts=conflicts,
                                   multi_author=multi_author,
                                   ci_failures=ci_failures,
                                   maintainers=maintainers,
                                   labels=labels,
                                   keywords=keywords,
                                   support_levels=support_levels,
                                   title=config['title'],
                                   now=now, **classes)

        html_filename = 'htmlout/%s.html' % tmplfile
        log.info('writing rendered html to %s', html_filename)
        with open(html_filename, 'w+b') as f:
            f.write(rendered.encode('ascii', 'ignore'))

        if config.get('use_rackspace', False):
            cont.upload_file('htmlout/%s.html' % tmplfile,
                             obj_name='%s.html' % tmplfile,
                             content_type='text/html')


def paths_to_prs(key, paths, files, dirs):
    prs = set([])
    for path in paths:
        prs.update(files.get(path, []))
        prs.update(dirs.get(path, []))
    return prs


def build_maps_to_prs(issue_data, maintainer_map):
    files = issue_data[1]
    dirs = issue_data[8]

    to_prs_map = {}
    for map_type in maintainer_map:
        type_to_pr_map = {}
        for key, paths in maintainer_map[map_type].items():
            # log.info('key: %s', key)
            prs = paths_to_prs(key, paths, files, dirs)
            type_to_pr_map[key] = prs
            # log.info('prs: %s', prs)

        to_prs_map[map_type] = type_to_pr_map

    # log.info('to_prs_map: %s', pprint.pformat(to_prs_map))
    return to_prs_map


if __name__ == '__main__':
    import sys

    config = get_config()

    use_cache = False
    fetch_data = True

    cached_data = None

    if '--cached' in sys.argv:
        use_cache = True
        fetch_data = True
    if '--only-cached' in sys.argv:
        use_cache = True
        fetch_data = False

    if use_cache:
        log.info('using cached data')
        data_file_name = get_data_path(config)
        with open(data_file_name, 'r') as f:
            cached_data = cPickle.load(f)
        log.info('loaded cached data')

    if fetch_data:
        data = scan_issues(get_config(), cached_data=cached_data)
    else:
        # cached_data may be none/empty
        data = cached_data

    maintainer_to_prs = defaultdict(list)

    botmeta_map = read_maintainers()
    other_data = build_maps_to_prs(data, botmeta_map)

    # for key, value in maintainer_to_paths.items():
    #    print key, value

    # log.info('other_data[maintainer]: %s', pprint.pformat(other_data['maintainer']))
    data.append(other_data['maintainer'])
    data.append(other_data['label'])
    data.append(other_data['keyword'])
    data.append(other_data['support_level'])

    write_html(*data)
