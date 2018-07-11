# Script to run to update our schema-registry

import avro.schema
import glob
import os
import json
import datetime

def checkDefault(schema):
    schema = avro.schema.Parse(open(schema, "rb").read())
    for field in schema.fields:
        if field.has_default is not True:
            print('Field ', field.name, ' in schema', schema.name, 'has no default value, please add one')
            return False
    return True

# schema name => transaction.avsc
def checkPrevious(schemaName, currentPath, versionsPath):
    pwd = os.path.dirname(__file__)
    currentSchema = json.load(open(os.path.join(pwd, currentPath, schemaName + '.avsc')))
    path = os.path.join(pwd, versionsPath, '*.avsc')
    schemas = glob.glob(path)
    print('----------- Schemas to check: ', len(schemas), '\n', schemas)
    for schema in schemas:
        print("Checked",schema)
        schemaAvro = json.load(open(schema))
        isValid = compareSchemas(currentSchema, schemaAvro)
        if not isValid:
            print('Proposed schema is not forward compatible with schema version', schema)
            return False
        print(schema, ' Is valid!')
    createVersion(currentSchema, schemaName)
    return True

def createVersion(schema, schemaName):
    pwd = os.path.dirname(__file__)
    versionName = datetime.datetime.utcnow().strftime("%Y%m%d%H%M") + "-" + schemaName
    versionFileName = os.path.join(pwd, 'versions', schemaName, versionName + '.avsc')
    os.makedirs(os.path.dirname(versionFileName), exist_ok=True)
    
    with open(versionFileName, 'w') as file:
        json.dump(schema, file)
    
    print("New version of the schema created: " + versionFileName)


def schemaToSet(schema):
    return set([field['name'] for field in schema['fields']])


def compareSchemas(currentSchema, toCompare):
    sameFlag = True
    crtSchemaSet = schemaToSet(currentSchema)
    toCompareSet = schemaToSet(toCompare)
    for fieldCrt in crtSchemaSet:
        if not fieldCrt in toCompareSet:
            print('Field ',fieldCrt,' exists in latest pushed version of the schema but not in the new version')
            sameFlag = False
    return sameFlag
    

def main():
    listSchemas = ['transaction', 'transactionCategorisee', 'transactionKey']
    pwd = os.path.dirname(__file__)
    for s in listSchemas:
        if checkDefault(os.path.join(pwd, '../../src/main/avro/', s + '.avsc')) is True:
            checkPrevious(s, '../../src/main/avro/', os.path.join('versions', s))
        else:
            break
    # checkDefault(schema)
    # checkPrevious('transaction', '../../src/main/avro/', 'versions/transaction')
    # pass


if __name__ == '__main__':
    main()