import logging
import os
import uuid
from typing import Callable

from comfy_web.compat import web

import folder_paths
from app.assets.api.schemas_in import ParsedUpload, UploadError
from app.assets.helpers import validate_blake3_hash


def normalize_and_validate_hash(s: str) -> str:
    """Validate and normalize a hash string.

    Returns canonical 'blake3:<hex>' or raises UploadError.
    """
    try:
        return validate_blake3_hash(s)
    except ValueError:
        raise UploadError(400, "INVALID_HASH", "hash must be like 'blake3:<hex>'")


async def parse_multipart_upload(
    request: web.Request,
    check_hash_exists: Callable[[str], bool],
) -> ParsedUpload:
    """
    Parse a multipart/form-data upload request.

    Args:
        request: The aiohttp request
        check_hash_exists: Callable(hash_str) -> bool to check if a hash exists

    Returns:
        ParsedUpload with parsed fields and temp file path

    Raises:
        UploadError: On validation or I/O errors
    """
    if not (request.content_type or "").lower().startswith("multipart/"):
        raise UploadError(
            415, "UNSUPPORTED_MEDIA_TYPE", "Use multipart/form-data for uploads."
        )

    post = await request.post()

    file_field = post.get("file")
    file_present = file_field is not None
    file_client_name: str | None = None
    tags_raw = request._request.form.getlist("tags") if request._request.form else []
    provided_name = post.get("name")
    user_metadata_raw = post.get("user_metadata")
    provided_hash = None
    provided_hash_exists: bool | None = None
    provided_mime_type = post.get("mime_type")
    provided_preview_id = post.get("preview_id")

    if "id" in post:
        raise UploadError(
            400,
            "UNSUPPORTED_FIELD",
            "Client-provided 'id' is not supported. Asset IDs are assigned by the server.",
        )

    hash_value = post.get("hash")
    if hash_value:
        try:
            provided_hash = normalize_and_validate_hash(str(hash_value).strip().lower())
        except Exception:
            raise UploadError(400, "INVALID_HASH", "hash must be like 'blake3:<hex>'")
        try:
            provided_hash_exists = check_hash_exists(provided_hash)
        except Exception as e:
            logging.exception("check_hash_exists failed for hash=%s: %s", provided_hash, e)
            raise UploadError(
                500,
                "HASH_CHECK_FAILED",
                "Backend error while checking asset hash.",
            )

    file_written = 0
    tmp_path: str | None = None

    if file_present:
        file_client_name = (file_field.filename or "").strip()
        file_body = file_field.file.read()
        file_written = len(file_body)
        file_field.file.seek(0)

        if not (provided_hash and provided_hash_exists is True):
            uploads_root = os.path.join(folder_paths.get_temp_directory(), "uploads")
            unique_dir = os.path.join(uploads_root, uuid.uuid4().hex)
            os.makedirs(unique_dir, exist_ok=True)
            tmp_path = os.path.join(unique_dir, ".upload.part")

            try:
                with open(tmp_path, "wb") as f:
                    f.write(file_body)
            except Exception:
                delete_temp_file_if_exists(tmp_path)
                raise UploadError(
                    500, "UPLOAD_IO_ERROR", "Failed to receive and store uploaded file."
                )

    if not file_present and not (provided_hash and provided_hash_exists):
        raise UploadError(
            400, "MISSING_FILE", "Form must include a 'file' part or a known 'hash'."
        )

    if (
        file_present
        and file_written == 0
        and not (provided_hash and provided_hash_exists)
    ):
        delete_temp_file_if_exists(tmp_path)
        raise UploadError(400, "EMPTY_UPLOAD", "Uploaded file is empty.")

    return ParsedUpload(
        file_present=file_present,
        file_written=file_written,
        file_client_name=file_client_name,
        tmp_path=tmp_path,
        tags_raw=tags_raw,
        provided_name=provided_name,
        user_metadata_raw=user_metadata_raw,
        provided_hash=provided_hash,
        provided_hash_exists=provided_hash_exists,
        provided_mime_type=provided_mime_type,
        provided_preview_id=provided_preview_id,
    )


def delete_temp_file_if_exists(tmp_path: str | None) -> None:
    """Safely remove a temp file and its parent directory if empty."""
    if tmp_path:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError as e:
            logging.debug("Failed to delete temp file %s: %s", tmp_path, e)
        try:
            parent = os.path.dirname(tmp_path)
            if parent and os.path.isdir(parent):
                os.rmdir(parent)  # only succeeds if empty
        except OSError:
            pass
