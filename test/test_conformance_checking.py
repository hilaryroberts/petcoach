import unittest
import spark_tools.conformance_checking as cfc


class UnitTest(unittest.TestCase):

    def test_add_place(self):
        test_net = cfc.Net()
        test_net.add_place('p1', 'Start')
        self.assertEquals(test_net.places['p1'], {'name': 'Start'})

    def test_add_transition(self):
        test_net = cfc.Net()
        test_net.add_transition('t1', 'sample transition', input_places=['p1'], output_places=['p2', 'p3'])
        self.assertEquals(test_net.transition_lookup['sample transition'], ['t1'])
        self.assertEquals(test_net.transitions['t1'].input_places, ['p1'])
        self.assertEquals(test_net.transitions['t1'].output_places, ['p2', 'p3'])

    def test_add_arc_into_transition(self):
        test_net = cfc.Net()
        test_net.add_transition('t1', 'sample transition')
        test_net.add_arc('p1', 't1')
        self.assertIn('p1', test_net.transitions['t1'].input_places)

    def test_add_arc_out_of_transition(self):
        test_net = cfc.Net()
        test_net.add_transition('t1', 'sample transition')
        test_net.add_arc('t1', 'p2')
        self.assertIn('p2', test_net.transitions['t1'].output_places)

    def test_fire_transition_by_name_single_option(self):
        test_net = cfc.Net()
        test_net.add_transition('t1', 'sample transition', input_places=['p1'], output_places=['p2'])
        test_marking = cfc.Marking()
        test_marking.produce_token('p1')
        test_net.fire_transition_by_name('sample transition', test_marking)
        self.assertIn('p2', test_marking.tokens)

    def test_fire_transition_by_name_single_legal_option(self):
        test_net = cfc.Net()
        test_net.add_transition('t1', 'sample transition', input_places=['p1'], output_places=['p4'])
        test_net.add_transition('t2', 'sample transition', input_places=['p2'], output_places=['p5'])
        test_net.add_transition('t3', 'sample transition', input_places=['p3'], output_places=['p6'])
        test_net.add_transition('t4', 'different transition', input_places=['p1'], output_places=['p7'])
        test_marking = cfc.Marking()
        test_marking.produce_token('p1')
        test_net.fire_transition_by_name('sample transition', test_marking)
        self.assertIn('p4', test_marking.tokens)
        self.assertNotIn('p5', test_marking.tokens)
        self.assertNotIn('p6', test_marking.tokens)
        self.assertNotIn('p7', test_marking.tokens)

    def test_fire_transition_by_name_multiple_illegal_options(self):
        test_net = cfc.Net()
        test_net.add_transition('t2', 'sample transition', input_places=['p1', 'p2'], output_places=['p6'])
        test_net.add_transition('t1', 'sample transition', input_places=['p1', 'p2', 'p3'], output_places=['p5'])
        test_net.add_transition('t3', 'sample transition', input_places=['p4'], output_places=['p7'])
        test_net.add_transition('t4', 'different transition', input_places=['p1', 'p3'], output_places=['p8'])
        test_marking = cfc.Marking()
        test_marking.produce_token('p1')
        test_marking.produce_token('p3')
        test_net.fire_transition_by_name('sample transition', test_marking)
        self.assertIn('p5', test_marking.tokens)
        self.assertNotIn('p6', test_marking.tokens)
        self.assertNotIn('p7', test_marking.tokens)
        self.assertNotIn('p8', test_marking.tokens)

    def test_fire_transition_by_name_multiple_legal_options(self):
        test_net = cfc.Net()
        test_net.add_transition('t1', 'sample transition', input_places=['p1'])
        test_net.add_transition('t2', 'sample transition', input_places=['p1', 'p2'])
        test_net.add_transition('t3', 'sample transition', input_places=['p3'])
        test_net.add_transition('t4', 'different transition', input_places=['p1', 'p2'], output_places=['p8'])
        test_marking = cfc.Marking()
        test_marking.produce_token('p1')
        test_marking.produce_token('p2')
        with self.assertRaises(cfc.MultiPathError) as context:
            test_net.fire_transition_by_name('sample transition', test_marking)
            self.assertTrue(
                context.possible_paths == ['t1', 't2'],
                msg="found multiple fireable transitions: ['t1', 't2']"
            )

    def test_fire_with_missing(self):
        test_net = cfc.Net()
        test_net.add_transition('t1', 'sample transition', input_places=['p1'], output_places=['p2'])
        test_marking = cfc.Marking()
        test_net.fire_transition('t1', test_marking)
        self.assertIn('p2', test_marking.tokens)
        self.assertIn('p1', test_marking.missing_tokens)

    def test_test_transition(self):
        test_net = cfc.Net()
        test_net.add_transition('t1', 'sample transition', input_places=['p1'], output_places=['p2'])
        test_marking = cfc.Marking()
        result = test_net.test_transition('t1', test_marking)
        self.assertEquals(1, result)

    def test_add_input_place(self):
        test_transition = cfc.Transition('t1', 'sample transition', [], [])
        test_transition.add_input_place('p1')
        self.assertIn('p1', test_transition.input_places)

    def test_add_output_place(self):
        test_transition = cfc.Transition('t1', 'sample transition', [], [])
        test_transition.add_output_place('p1')
        self.assertIn('p1', test_transition.output_places)

    def test_has_token(self):
        test_marking = cfc.Marking()
        test_marking.produce_token('p1')
        self.assertTrue(test_marking.has_token('p1'))

    def test_produce_token(self):
        test_marking = cfc.Marking()
        test_marking.produce_token('p1')
        self.assertIn('p1', test_marking.tokens)

    def test_consume_token(self):
        test_marking = cfc.Marking()
        test_marking.produce_token('p1')
        test_marking.consume_token('p1')
        self.assertNotIn('p1', test_marking.tokens)

    def test_marking_equality(self):
        test_marking = cfc.Marking()
        test_marking.produce_token('p1')
        test_marking.produce_token('p2')
        test_marking2 = cfc.Marking()
        test_marking2.produce_token('p2')
        test_marking2.produce_token('p1')
        self.assertEquals(test_marking, test_marking2)

    def test_parse_pnml(self):
        parsed_net = cfc.parse_pnml('test/fixtures/test_net1.pnml')
        self.assertEquals(['t1'], parsed_net.transition_lookup['go left'])
        self.assertEquals(['t2'], parsed_net.transition_lookup['go right'])
        self.assertEquals(['t3'], parsed_net.transition_lookup['turn back'])
        self.assertEquals(['t4'], parsed_net.transition_lookup['stop'])
        self.assertEquals(['p1'], parsed_net.transitions['t1'].input_places)
        self.assertIn('p2', parsed_net.transitions['t1'].output_places)
        self.assertIn('p3', parsed_net.transitions['t1'].output_places)
        self.assertEquals(['p1'], parsed_net.transitions['t2'].input_places)
        self.assertIn('p2', parsed_net.transitions['t2'].output_places)
        self.assertIn('p4', parsed_net.transitions['t2'].output_places)
        self.assertEquals(['p3'], parsed_net.transitions['t3'].input_places)
        self.assertEquals(['p4'], parsed_net.transitions['t3'].output_places)
        self.assertIn('p2', parsed_net.transitions['t4'].input_places)
        self.assertIn('p4', parsed_net.transitions['t4'].input_places)
        self.assertEquals(['p5'], parsed_net.transitions['t4'].output_places)
        self.assertIn('p1', parsed_net.initial_marking.tokens)

    def test_check_events(self):
        events = ['go right', 'stop']
        test_net = cfc.parse_pnml('test/fixtures/test_net1.pnml')
        checker = cfc.LogChecker(test_net, events)
        checker.check_events()
        self.assertIn('p5', checker.path_buffer[0].tokens)

    def test_check_events_invalid(self):
        events = ['go left', 'stop']
        test_net = cfc.parse_pnml('test/fixtures/test_net1.pnml')
        checker = cfc.LogChecker(test_net, events)
        checker.check_events()
        self.assertIn('p5', checker.path_buffer[0].tokens)
        self.assertIn('p4', checker.path_buffer[0].missing_tokens)
        self.assertIn('p3', checker.path_buffer[0].tokens)

    def test_update_markings(self):
        test_net = cfc.parse_pnml('test/fixtures/test_net1.pnml')
        checker = cfc.LogChecker(test_net, [])
        checker.update_markings('go right')
        self.assertIn('p2', checker.path_buffer[0].tokens)
        self.assertIn('p4', checker.path_buffer[0].tokens)
        self.assertNotIn('p1', checker.path_buffer[0].tokens)

    def test_trim_path_buffer(self):
        checker = cfc.LogChecker(cfc.Net(), [])
        good_marking = cfc.Marking()
        bad_marking = cfc.Marking()
        duplicate_marking = cfc.Marking()
        good_marking.tokens = ['p1', 'p2']
        duplicate_marking.tokens = ['p1', 'p2']
        bad_marking.tokens = ['p3']
        bad_marking.missing_tokens = ['p4']
        checker.path_buffer = [good_marking, bad_marking, duplicate_marking]
        checker.trim_path_buffer()
        self.assertEquals(len(checker.path_buffer), 1)
        self.assertEquals(['p1', 'p2'], checker.path_buffer[0].tokens)

    def test_keep_least_divergent_markings(self):
        checker = cfc.LogChecker(cfc.Net(), [])
        best_marking = cfc.Marking()
        bad_marking1 = cfc.Marking()
        bad_marking2 = cfc.Marking()
        best_marking.tokens = ['p1']
        bad_marking1.tokens = ['p2']
        bad_marking2.tokens = ['p2']
        best_marking.missing_tokens = ['p4']
        bad_marking1.missing_tokens = ['p4', 'p5']
        bad_marking2.missing_tokens = ['p4', 'p5', 'p6']
        checker.path_buffer = [best_marking, bad_marking1, bad_marking2]
        checker.keep_least_divergent_markings()
        self.assertEquals(len(checker.path_buffer), 1)
        self.assertEquals(['p1'], checker.path_buffer[0].tokens)

    def test_delete_dupllicate_markings(self):
        checker = cfc.LogChecker(cfc.Net(), [])
        marking = cfc.Marking()
        marking.tokens = ['p1']
        checker.path_buffer = [marking, marking, marking]
        checker.delete_duplicate_markings()
        self.assertEquals(len(checker.path_buffer), 1)

    def test_resolve_expected_final_marking(self):
        checker = cfc.LogChecker(cfc.Net(), [])
        marking = cfc.Marking()
        marking.tokens = ['p1']
        checker.path_buffer = [marking, marking]
        output = checker.resolve_expected_final_marking(['p1'])
        self.assertEquals(len(output.tokens), 0)

    def test_format_results(self):
        input_marking = cfc.Marking
        input_marking.tokens = ['p1', 'p3']
        input_marking.missing_tokens = ['p2']
        output = cfc.format_results(input_marking)
        self.assertEquals(output, {
        'missing_tokens': ['p2'],
        'unconsumed_tokens': ['p1', 'p3']})

    def test_check_log_single_valid_path(self):
        test_net = cfc.parse_pnml('test/fixtures/test_net1.pnml')
        events = ['go left', 'turn back', 'stop']
        output = cfc.check_log(test_net, events, expected_final_marking=['p5'])
        self.assertEquals(len(output['missing_tokens']),0)
        self.assertEquals(len(output['unconsumed_tokens']),0)

    def test_check_log_single_bad_path(self):
        test_net = cfc.parse_pnml('test/fixtures/test_net1.pnml')
        events = ['go left', 'stop']
        output = cfc.check_log(test_net, events, expected_final_marking=['p5'])
        self.assertEquals(output['missing_tokens'], ['p4'])
        self.assertEquals(output['unconsumed_tokens'], ['p3'])

    def test_check_log_multiple_paths_some_valid(self):
        test_net = cfc.parse_pnml('test/fixtures/test_net3.pnml')
        events = ['wake up', 'have coffee', 'go to work', 'start working']
        output = cfc.check_log(test_net, events, expected_final_marking=['p11'])
        self.assertEquals(len(output['missing_tokens']),0)
        self.assertEquals(len(output['unconsumed_tokens']),0)

    def test_check_log_multiple_paths_none_valid(self):
        test_net = cfc.parse_pnml('test/fixtures/test_net3.pnml')
        events = ['wake up', 'go for a run', 'change', 'go to work', 'start working']
        output = cfc.check_log(test_net, events, expected_final_marking=['p11'])
        self.assertEquals(output['missing_tokens'], ['p8'])
        self.assertEquals(output['unconsumed_tokens'], ['p7'])


if __name__ == '__main__':
    unittest.main()
