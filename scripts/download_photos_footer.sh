__ICBAR_PHOTO_MANIFEST_9E7C62A1__
}

if [[ "${1:-}" == '--count' ]]; then
  manifest | wc -l
  exit 0
fi
if [[ "${1:-}" == '--list' ]]; then
  manifest
  exit 0
fi
mkdir -p "$OUTPUT_DIR"
echo "Saving images in: $OUTPUT_DIR (parallel workers: $JOBS)"
# xargs launches bounded concurrent curl jobs. Running again skips completed images.
manifest | xargs -r -d '\n' -n 1 -P "$JOBS" bash -c 'fetch_photo "$1"' _
echo 'Photo download pass finished.'
