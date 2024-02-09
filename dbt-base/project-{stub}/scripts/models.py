from typing import List, Dict, Optional, AnyStr
from pydantic import BaseModel, constr

class IncludeExclude(BaseModel):
    include: Optional[AnyStr]
    exclude: Optional[List[str]]

class DataTest(BaseModel):
    test: str
    description: Optional[str]
    severity: Optional[str]
    warn_on: Optional[str]
    error_on: Optional[str]

class Column(BaseModel):
    name: constr(min_length=1)
    description: Optional[str]
    data_type: str
    meta: Optional[Dict[str, Optional[str]]]
    quote: Optional[bool]
    constraints: Optional[List[Dict[str, Optional[str]]]]
    tests: Optional[List[DataTest]]
    tags: Optional[List[str]]

class ModelConfig(BaseModel):
    show: Optional[bool]
    node_color: Optional[Union[str, AnyStr]]

class ModelVersion(BaseModel):
    v: str
    defined_in: Optional[str]
    description: Optional[str]
    docs: Optional[ModelConfig]
    access: Optional[str]
    constraints: Optional[List[Dict[str, Optional[str]]]]
    config: Optional[Dict[str, Optional[str]]]
    tests: Optional[List[DataTest]]
    columns: Optional[List[Union[IncludeExclude, Column]]]

class Model(BaseModel):
    name: constr(min_length=1)
    description: Optional[str]
    docs: Optional[ModelConfig]
    latest_version: Optional[str]
    deprecation_date: Optional[str]
    access: Optional[str]
    config: Optional[Dict[str, Optional[str]]]
    constraints: Optional[List[Dict[str, Optional[str]]]]
    tests: Optional[List[DataTest]]
    columns: Optional[List[Column]]
    versions: Optional[List[ModelVersion]]
