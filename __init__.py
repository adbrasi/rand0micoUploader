# __init__.py

from .hf_batch_uploader import HuggingFaceBatchUploader

NODE_CLASS_MAPPINGS = {
    "HuggingFaceBatchUploader": HuggingFaceBatchUploader
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "HuggingFaceBatchUploader": "HuggingFace Batch Uploader"
}

__all__ = ['NODE_CLASS_MAPPINGS', 'NODE_DISPLAY_NAME_MAPPINGS']

print("✅ Custom Node: HuggingFace Batch Uploader loaded.")