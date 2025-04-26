from unittest import TestCase
import binascii
import metadata

ROFLCOPTER_TEST_STRING = '00000000000000000000004f0000000102b069457c00000000000000000191e05af81800000191e05af818ffffffffffffffffffffffffffff000000013a000000012e010c00116d657461646174612e76657273696f6e001400000000000000000001000000e4000000010224db12dd00000000000200000191e05b2d1500000191e05b2d15ffffffffffffffffffffffffffff000000033c00000001300102000473617a00000000000040008000000000000091000090010000020182010103010000000000000000000040008000000000000091020000000102000000010101000000010000000000000000021000000000004000800000000000000100009001000004018201010301000000010000000000004000800000000000009102000000010200000001010100000001000000000000000002100000000000400080000000000000010000'

class TestRecordValue(TestCase):
    def test_of_bytes(self):
        stuff = binascii.unhexlify(ROFLCOPTER_TEST_STRING)
        parsed = metadata.ClusterMetaDataLog.of_bytes(stuff)
        batch_1 = parsed.record_batches[0]
        self.assertEqual(batch_1.batch_length, 79)
        self.assertEqual(batch_1.partition_leader_epoch, 1)
        self.assertEqual(batch_1.magic_byte, 2)
        self.assertEqual(batch_1.crc, -1335278212)
        self.assertEqual(batch_1.compression, metadata.Compression.NO_COMPRESSION)
        self.assertFalse(batch_1.timestamp_type)
        self.assertFalse(batch_1.is_transactional)
        self.assertFalse(batch_1.is_control_batch)
        self.assertFalse(batch_1.has_delete_horizon)
        self.assertEqual(batch_1.last_offset_delta, 0)
        self.assertIsNone(batch_1.base_timestamp)
        self.assertIsNone(batch_1.max_timestamp)
        self.assertEqual(batch_1.producer_id, -1)
        self.assertEqual(batch_1.producer_epoch, -1)
        self.assertEqual(batch_1.base_sequence, -1)
        self.assertEqual(batch_1.records_length, 1)



        self.assertEqual(parsed.record_batches, 2)

        self.fail()

    def test_parse_uuid(self):
        stuff = binascii.unhexlify('00000000000040008000000000000001')
        parser = metadata._Parser(stuff)
        id = parser.parse_uuid()
        self.assertIsNotNone(id)

    def test_varint(self):
        stuff = binascii.unhexlify('9001')
        parser = metadata._Parser(stuff)
        res = parser.read_zig_zag(signed=True)
        self.assertEqual(72, res)

        stuff = binascii.unhexlify('9001')
        parser = metadata._Parser(stuff)
        res = parser.read_zig_zag(signed=True)
        self.assertFalse(parser.has_next())

        stuff = binascii.unhexlify('900100')
        parser = metadata._Parser(stuff)
        res = parser.read_zig_zag(signed=True)
        self.assertEqual(72, res)
        self.assertTrue(parser.has_next())

        stuff = binascii.unhexlify('3c')
        parser = metadata._Parser(stuff)
        res = parser.read_zig_zag(signed=True)
        self.assertEqual(30, res)


        stuff = binascii.unhexlify('3c')
        parser = metadata._Parser(stuff)
        res = parser.read_zig_zag(signed=True)
        self.assertFalse(parser.has_next())

