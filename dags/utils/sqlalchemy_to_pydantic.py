from typing import Any, Dict, List, Optional, Type, Union

from pydantic import BaseModel, Field, create_model
from pydantic.config import ConfigDict
from sqlalchemy.orm import DeclarativeMeta

def sqlalchemy_to_pydantic(model: Type[DeclarativeMeta]) -> Type[BaseModel]:
    """
    Generates a Pydantic model based on a SQLAlchemy model.

    Args:
        model (Type[DeclarativeMeta]): SQLAlchemy model.

    Returns:
        Type[BaseModel]: Generated Pydantic model.
    """

    def get_field_type(column):
        """Determines the field type based on the SQLAlchemy type."""
        python_type = getattr(column.type, "python_type", None)
        if python_type is dict:
            return Optional[Union[List[Dict[str, Any]], Dict[str, Any]]]
        return Optional[python_type] if column.nullable else python_type

    fields = {
        column.name: (
            get_field_type(column),
            Field(default=None if column.nullable else ...),
        )
        for column in model.__table__.columns
    }

    model_config = ConfigDict(
        from_attributes=True,
    )
    return create_model(
        f"{model.__name__}PydanticSchema", __config__=model_config, **fields
    )
