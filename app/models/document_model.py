from pydantic import BaseModel, Field, ConfigDict, field_serializer
from pydantic_core import core_schema
from typing import List, Optional, Dict, Any, Union, Annotated
from datetime import datetime
from bson import ObjectId


class PyObjectId(ObjectId):
    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        return core_schema.union_schema(
            [
                core_schema.is_instance_schema(ObjectId),
                core_schema.chain_schema(
                    [
                        core_schema.str_schema(),
                        core_schema.no_info_plain_validator_function(cls.validate),
                    ]
                ),
            ],
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda x: str(x)
            ),
        )

    @classmethod
    def validate(cls, v):
        if isinstance(v, ObjectId):
            return v
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)


class DocumentModel(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )
    
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    file_name: str
    original_filename: str
    file_path: str
    upload_date: datetime = Field(default_factory=datetime.now)
    file_size: int
    comparison_id: str
    comparison_data: Dict[str, Any]
    extracted_quotes: List[Dict[str, Any]]
    summary: Dict[str, Any]
    key_differences: Union[List[Dict[str, Any]], Dict[str, Any]]
    side_by_side: Dict[str, Any]
    data_table: Union[List[Dict[str, Any]], Dict[str, Any]]
    analytics: Dict[str, Any]
    provider_cards: List[Dict[str, Any]]
    processing_timestamp: datetime
    status: str = "completed"  # completed, processing, failed
    
    @field_serializer('id')
    def serialize_id(self, value: PyObjectId, _info):
        return str(value)
    
    @field_serializer('upload_date', 'processing_timestamp')
    def serialize_datetime(self, value: datetime, _info):
        return value.isoformat()


class DocumentCreate(BaseModel):
    file_name: str
    original_filename: str
    file_path: str
    file_size: int
    comparison_id: str
    comparison_data: Dict[str, Any]
    extracted_quotes: List[Dict[str, Any]]
    summary: Dict[str, Any]
    key_differences: Union[List[Dict[str, Any]], Dict[str, Any]]
    side_by_side: Dict[str, Any]
    data_table: Union[List[Dict[str, Any]], Dict[str, Any]]
    analytics: Dict[str, Any]
    provider_cards: List[Dict[str, Any]]
    processing_timestamp: datetime
    status: str = "completed"


class DocumentResponse(BaseModel):
    model_config = ConfigDict()
    
    id: str
    file_name: str
    original_filename: str
    upload_date: datetime
    file_size: int
    comparison_id: str
    status: str
    summary: Optional[Dict[str, Any]] = None
    
    @field_serializer('upload_date')
    def serialize_datetime(self, value: datetime, _info):
        return value.isoformat()


class DocumentDetailResponse(BaseModel):
    model_config = ConfigDict()
    
    id: str
    file_name: str
    original_filename: str
    upload_date: datetime
    file_size: int
    comparison_id: str
    comparison_data: Dict[str, Any]
    extracted_quotes: List[Dict[str, Any]]
    summary: Dict[str, Any]
    key_differences: Union[List[Dict[str, Any]], Dict[str, Any]]
    side_by_side: Dict[str, Any]
    data_table: Union[List[Dict[str, Any]], Dict[str, Any]]
    analytics: Dict[str, Any]
    provider_cards: List[Dict[str, Any]]
    processing_timestamp: datetime
    status: str
    
    @field_serializer('upload_date', 'processing_timestamp')
    def serialize_datetime(self, value: datetime, _info):
        return value.isoformat()