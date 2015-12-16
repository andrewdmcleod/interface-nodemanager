# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

from charms.reactive import RelationBase
from charms.reactive import hook
from charms.reactive import scopes
from charms.reactive.bus import get_states

from charmhelpers.core import hookenv


class NodeManagerProvides(RelationBase):
    scope = scopes.GLOBAL
    auto_accessors = ['host', 'resourcemanager_port', 'hs_http', 'hs_ipc', 'ssh-key']

    def set_nodemanager_spec(self, spec):
        """
        Set the local spec.

        Should be called after ``{relation_name}.related``.
        """
        conv = self.conversation()
        conv.set_local('spec', json.dumps(spec))

    def local_hostname(self):
        return hookenv.local_unit().replace('/', '-')

    def nodemanager_spec(self):
        conv = self.conversation()
        return json.loads(conv.get_local('spec', '{}'))

    def resourcemanager_spec(self):
        conv = self.conversation()
        return json.loads(conv.get_remote('spec', '{}'))

    def hosts_map(self):
        conv = self.conversation()
        return json.loads(conv.get_remote('hosts-map', '{}'))

    @hook('{provides:nodemanager}-relation-joined')
    def joined(self):
        conv = self.conversation()
        conv.set_state('{relation_name}.related')

    @hook('{provides:nodemanager}-relation-changed')
    def changed(self):
        hookenv.log('Data: {}'.format({
            'spec': self.spec(),
            'host': self.host(),
            'resourcemanager_port': self.resourcemanager_port(),
            'hs_http': self.hs_http(),
            'hs_ipc': self.hs_ipc(),
            'hosts_map': self.hosts_map(),
            'local_hostname': self.local_hostname(),
        }))
        conv = self.conversation()
        available = all([self.spec(), self.host(), self.resourcemanager_port(), self.hs_http(), self.hs_ipc(), self.ssh_key()])
        spec_matches = self._spec_match()
        registered = self.local_hostname() in self.hosts_map().values()

        conv.toggle_state('{relation_name}.spec.mismatch', available and not spec_matches)
        conv.toggle_state('{relation_name}.ready', available and spec_matches and registered)

        hookenv.log('States: {}'.format(get_states().keys()))

    def register(self):
        conv = self.conversation()
        conv.set_remote('registered', 'true')

    @hook('{provides:nodemanager}-relation-{departed,broken}')
    def departed(self):
        conv = self.conversation()
        conv.remove_state('{relation_name}.related')
        conv.remove_state('{relation_name}.spec.mismatch')
        conv.remove_state('{relation_name}.ready')

    def _spec_match(self):
        datanode_spec = self.datanode_spec()
        namenode_spec = self.namenode_spec()
        for key, value in datanode_spec.items():
            if value != namenode_spec.get(key):
                return False
        return True
