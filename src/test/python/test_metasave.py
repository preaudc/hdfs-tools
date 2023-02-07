from unittest import TestCase, mock

from hdfs_tools.metasave import Metasave

class TestMetaSave(TestCase):

    def test_metasave_l1_d0(self):
        with mock.patch('socket.gethostbyaddr', side_effect=[['datanode-01']]):
            nn_metasave = Metasave('src/test/resources/hadoop-hdfs-metasave.log', {'live':'1', 'decommissioned':'0'})
            nn_metasave.parse()
            self.assertEqual(len(nn_metasave.replica_count_by_host), 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-01'], 12)

    def test_metasave_l1_d1(self):
        with mock.patch(
            'socket.gethostbyaddr',
            side_effect=[
                ['datanode-01'],
                ['datanode-02'],
                ['datanode-03'],
                ['datanode-04'],
                ['datanode-05'],
                ['datanode-06']
            ]
        ):
            nn_metasave = Metasave('src/test/resources/hadoop-hdfs-metasave.log', {'live':'1', 'decommissioned':'1'})
            nn_metasave.parse()
            self.assertEqual(len(nn_metasave.replica_count_by_host), 6)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-01'], 69)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-02'], 87)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-03'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-04'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-05'], 4)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-06'], 12)

    def test_metasave_l0_d1(self):
        with mock.patch('socket.gethostbyaddr', side_effect=[['datanode-01']]):
            nn_metasave = Metasave('src/test/resources/hadoop-hdfs-metasave.log', {'live':'0', 'decommissioned':'1'})
            nn_metasave.parse()
            self.assertEqual(len(nn_metasave.replica_count_by_host), 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-01'], 1)

    def test_metasave_l2_d0(self):
        with mock.patch(
            'socket.gethostbyaddr',
            side_effect=[
                ['datanode-01'],
                ['datanode-02'],
                ['datanode-03'],
                ['datanode-04'],
                ['datanode-05'],
                ['datanode-06'],
                ['datanode-07']
            ]
        ):
            nn_metasave = Metasave('src/test/resources/hadoop-hdfs-metasave.log', {'live':'2', 'decommissioned':'0'})
            nn_metasave.parse()
            self.assertEqual(len(nn_metasave.replica_count_by_host), 7)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-01'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-02'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-03'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-04'], 2)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-05'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-06'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-07'], 1)

    def test_metasave_l2_d1(self):
        with mock.patch(
            'socket.gethostbyaddr',
            side_effect=[
                ['datanode-01'],
                ['datanode-02'],
                ['datanode-03'],
                ['datanode-04'],
                ['datanode-05'],
                ['datanode-06'],
                ['datanode-07'],
                ['datanode-08'],
                ['datanode-09'],
                ['datanode-10']
            ]
        ):
            nn_metasave = Metasave('src/test/resources/hadoop-hdfs-metasave.log', {'live':'2', 'decommissioned':'1'})
            nn_metasave.parse()
            self.assertEqual(len(nn_metasave.replica_count_by_host), 10)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-01'], 6)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-02'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-03'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-04'], 2)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-05'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-06'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-07'], 2)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-08'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-09'], 2)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-10'], 1)

    def test_metasave_l1_d1_c1(self):
        with mock.patch('socket.gethostbyaddr', side_effect=[['datanode-01'], ['datanode-02']]):
            nn_metasave = Metasave('src/test/resources/hadoop-hdfs-metasave.log', {'live':'1', 'decommissioned':'1', 'corrupt':'1'})
            nn_metasave.parse()
            nn_metasave.display_replica_count_by_host()
            self.assertEqual(len(nn_metasave.replica_count_by_host), 2)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-01'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-02'], 1)

    def test_metasave_l1_d0_c2(self):
        with mock.patch('socket.gethostbyaddr', side_effect=[['datanode-01']]):
            nn_metasave = Metasave('src/test/resources/hadoop-hdfs-metasave.log', {'live':'1', 'decommissioned':'0', 'corrupt':'2'})
            nn_metasave.parse()
            self.assertEqual(len(nn_metasave.replica_count_by_host), 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-01'], 1)

    def test_metasave_l1_d1_e1(self):
        with mock.patch('socket.gethostbyaddr', side_effect=[['datanode-01'], ['datanode-02']]):
            nn_metasave = Metasave('src/test/resources/hadoop-hdfs-metasave.log', {'live':'1', 'decommissioned':'1', 'excess':'1'})
            nn_metasave.parse()
            self.assertEqual(len(nn_metasave.replica_count_by_host), 2)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-01'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-02'], 1)

    def test_metasave_l1_d1_e2(self):
        with mock.patch('socket.gethostbyaddr', side_effect=[['datanode-01'], ['datanode-02']]):
            nn_metasave = Metasave('src/test/resources/hadoop-hdfs-metasave.log', {'live':'1', 'decommissioned':'1', 'excess':'2'})
            nn_metasave.parse()
            self.assertEqual(len(nn_metasave.replica_count_by_host), 2)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-01'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-02'], 1)

    def test_metasave_from_count(self):
        with mock.patch(
            'socket.gethostbyaddr',
            side_effect=[
                ['datanode-01'],
                ['datanode-02'],
                ['datanode-03'],
                ['datanode-04'],
                ['datanode-05'],
                ['datanode-06']
            ]
        ):
            args = mock.Mock()
            args.live = 1
            args.decommissioned = None
            args.corrupt = None
            args.excess = None
            nn_metasave = Metasave.from_count('src/test/resources/hadoop-hdfs-metasave.log', args)
            nn_metasave.parse()
            self.assertEqual(len(nn_metasave.replica_count_by_host), 6)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-01'], 81)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-02'], 87)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-03'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-04'], 1)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-05'], 4)
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-06'], 12)

    def test_metasave_no_live_replicas(self):
        with mock.patch('socket.gethostbyaddr', side_effect=[['datanode-01']]):
            args = mock.Mock()
            args.no_live_replicas = 'datanode-01'
            args.under_replicated = None
            nn_metasave = Metasave.from_block('src/test/resources/hadoop-hdfs-metasave.log', args)
            nn_metasave.parse()
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-01'], 1)

    def test_metasave_under_replicated(self):
        with mock.patch(
            'socket.gethostbyaddr',
            side_effect=[
                ['datanode-01'],
                ['datanode-02'],
                ['datanode-03'],
                ['datanode-04'],
                ['datanode-05'],
                ['datanode-06'],
                ['datanode-07'],
                ['datanode-08'],
                ['datanode-09'],
                ['datanode-10'],
                ['datanode-11'],
                ['datanode-12'],
                ['datanode-13']
            ]
        ):
            args = mock.Mock()
            args.no_live_replicas = None
            args.under_replicated = 'datanode-01'
            nn_metasave = Metasave.from_block('src/test/resources/hadoop-hdfs-metasave.log', args)
            nn_metasave.parse()
            self.assertEqual(nn_metasave.replica_count_by_host['datanode-01'], 70)
