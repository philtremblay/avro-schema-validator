import unittest
import avro.schema
import os
import json
from validateSchema.validateSchema import checkDefault, schemaToSet, compareSchemas, checkPrevious

class ValidateSchemaTests(unittest.TestCase):
    def setUp(self):
        relPath = os.path.dirname(__file__)
        self.schemaAllDefault = os.path.join(relPath,"data/user_allDefault")
        self.schemaNoDefault =  os.path.join(relPath,"data/user_noDefault")
        self.schemaJson = json.load(open(os.path.join(relPath, "data/user_noDefault.avsc"), "rb"))
        self.schemaJsonMissing = json.load(open(os.path.join(relPath, "data/user_missingField.avsc"), "rb"))
    
    def test_checkDefault_alltrue(self):
        self.assertEqual(checkDefault(self.schemaAllDefault), True)

    def test_checkDefault_someDefault(self):
        self.assertEqual(checkDefault(self.schemaNoDefault), False)

    def test_schemaToSet(self):
        self.assertSetEqual(schemaToSet(self.schemaJson), set(["name", "favorite_color", "favorite_number"]))
        self.assertIsNot(schemaToSet(self.schemaJson), set(["name"]))

    def test_compareSchemas(self):
        self.assertTrue(compareSchemas(self.schemaJson, self.schemaJson))
        # Case where we removed one from the old
        self.assertFalse(compareSchemas(self.schemaJson, self.schemaJsonMissing))
    
    def test_previous(self):
        self.assertFalse(checkPrevious('user_allDefault', '../test/data/', '../test/data/'))