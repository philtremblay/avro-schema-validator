# Script to validate schema
This scipt tests for forward and backward compatibility of Avro schema's

## To Run validation on all 3 schemas at the same time
**This will create a new version for each schema**
```python
python3 validateSchema/validateSchema.py 
```

### TODO 
Add command line arguments to specify schema

## Dev
To run the tests
```python
python3 -m unittest discover --verbose
```