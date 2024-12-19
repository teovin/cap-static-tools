import json
from datetime import datetime, timezone
from invoke import task
import pandas as pd

from .helpers import (get_volumes_metadata, get_reporter_volumes_metadata, R2_STATIC_BUCKET, R2_UNREDACTED_BUCKET,
                      RCLONE_R2_UNREDACTED_BASE_URL, RCLONE_R2_CAP_STATIC_BASE_URL, r2_paginator, write_paths_to_file,
                      write_volumes_to_file, VOLUMES_TO_UNREDACT_FILE, get_single_volume_metadata, r2_s3_client)


@task
def unredact_volumes(ctx, volume=None, reporter=None, publication_year=None):
    """
    Invoked with
    `invoke unredact.unredact-volumes --volume=32044109578716` or
    `invoke unredact.unredact-volumes --reporter=bta` or
    `invoke unredact.unredact-volumes --publication-year=1930`
    Creates a txt file with source and target path pairs which later will be used for rclone sync
    Creates a txt file with reporter and volume folder data which later will be used for metadata json file updates
    """
    passed_params = [param for param in [volume, reporter, publication_year] if param is not None]
    assert len(passed_params) == 1, "Cannot pass more than one parameter at a time."

    if volume:
        process_unredaction(volume, None, None)
    elif reporter:
        process_unredaction(None, reporter, None)
    elif publication_year:
        process_unredaction(None, None, publication_year)


@task
def update_redacted_field(ctx, dry_run=False):
    """
    The output of the unredact-volumes task is used to decide which volumes need updating.
    Updates the `redacted` flags in top level, reporter level and volume level volume metadata files.
    Updates the `last_updated` flag in top level, reporter level and volume level volume metadata files.
    Updates the `last_updated` flag in the volume level cases metadata json file.
    If dry-run is passed, won't update the files.
    """
    with open(VOLUMES_TO_UNREDACT_FILE, 'r') as volumes_file:
        if not bool(volumes_file.readlines()):
            raise Exception(f"Couldn't find any volumes in file.")

        volumes_file.seek(0)
        volumes_metadata = json.loads(get_volumes_metadata(R2_STATIC_BUCKET))

        # make a backup of top level VolumesMetadata.json file
        # just in case we need to restore it quickly in the event of a bug
        with open("VolumesMetadata_backup.json", 'w') as backup_file:
            json.dump(volumes_metadata, backup_file, indent=4)

        ### update the top level volumes metadata fields ###

        volumes_to_unredact = volumes_file.readlines()
        for vol in volumes_to_unredact:
            reporter_slug, volume_folder = map(str.strip, vol.split('/', 1))
            for volume in volumes_metadata:
                if reporter_slug == volume["reporter_slug"] and volume_folder == volume["volume_folder"]:
                    volume["redacted"] = False
                    volume["last_updated"] = datetime.now(timezone.utc).isoformat()

        # upload the new top level VolumesMetadata.json
        if not dry_run:
            r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Body=json.dumps(volumes_metadata),
                                    Key="VolumesMetadata.json", ContentType="application/json")
            print("Top level VolumesMetadata.json is updated.")

        ### update the reporter level volumes metadata fields ###

        df = pd.read_csv(VOLUMES_TO_UNREDACT_FILE, header=None, names=['volume_string'])
        df[['reporter', 'volume_folder']] = df['volume_string'].str.split('/', expand=True)
        grouped_volume_data = df.groupby('reporter')['volume_folder'].apply(list).to_dict()

        for reporter, volumes in grouped_volume_data.items():
            reporter_volumes_metadata = json.loads(get_reporter_volumes_metadata(R2_STATIC_BUCKET, reporter))
            for volume_folder in volumes:
                for volume in reporter_volumes_metadata:
                    if volume_folder == volume["volume_folder"]:
                        volume["redacted"] = False
                        volume["last_updated"] = datetime.now(timezone.utc).isoformat()

            # upload the new reporter level VolumesMetadata.json
            if not dry_run:
                r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Body=json.dumps(reporter_volumes_metadata),
                                        Key=f"{reporter}/VolumesMetadata.json", ContentType="application/json")
                print(f"Reporter level VolumesMetadata.json is updated for reporter {reporter}.")

        ### update the reporter volume level volumes metadata and cases metadata fields ###

        for reporter, volumes in grouped_volume_data.items():
            for volume_folder in volumes:
                volume_metadata = json.loads(get_single_volume_metadata(R2_STATIC_BUCKET, reporter, volume_folder, "VolumeMetadata"))
                cases_metadata = json.loads(get_single_volume_metadata(R2_STATIC_BUCKET, reporter, volume_folder, "CasesMetadata"))
                volume_metadata["redacted"] = False
                volume_metadata["last_updated"] = datetime.now(timezone.utc).isoformat()
                for case in cases_metadata:
                    case["last_updated"] = datetime.now(timezone.utc).isoformat()

                # upload the new reporter volume level VolumeMetadata.json and CasesMetadata.json
                if not dry_run:
                    r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Body=json.dumps(volume_metadata),
                                            Key=f"{reporter}/{volume_folder}/VolumeMetadata.json", ContentType="application/json")
                    print(f"Reporter volume level VolumeMetadata.json is updated for {reporter}/{volume_folder} volume.")
                    r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Body=json.dumps(cases_metadata),
                                            Key=f"{reporter}/{volume_folder}/CasesMetadata.json", ContentType="application/json")
                    print(f"Reporter volume level CasesMetadata.json is updated for {reporter}/{volume_folder} volume.")


def process_unredaction(volume, reporter, publication_year):
    """
    Helper function for the unredaction process
    """
    volumes_to_unredact, volume_matches = create_file_mappings_for_unredaction(volume, reporter, publication_year)
    print(f"{len(volumes_to_unredact)} volumes need to be unredacted.")
    if volume_matches:
        write_paths_to_file(volume_matches)
        write_volumes_to_file(volumes_to_unredact)

def create_file_mappings_for_unredaction(volume=None, reporter=None, publication_year=None):
    """
    Creates a list of volumes that need unredaction
    Creates a list of files that need to be copied to static bucket
    """
    if volume:
        unredacted_bucket_volumes = get_volumes_metadata(R2_UNREDACTED_BUCKET)
        static_bucket_volumes = get_volumes_metadata(R2_STATIC_BUCKET)
        unredacted_bucket_volume = [item for item in json.loads(unredacted_bucket_volumes) if item.get("id") == volume]
        static_bucket_volume = [item for item in json.loads(static_bucket_volumes) if item.get("id") == volume]

        if not unredacted_bucket_volume:
            raise Exception(f"Did not find the volume in {R2_UNREDACTED_BUCKET} bucket")

        if not static_bucket_volume:
            raise Exception(f"Did not find the volume in {R2_STATIC_BUCKET} bucket")

        return map_files_for_unredaction(static_bucket_volume, unredacted_bucket_volume)

    if reporter:
        unredacted_bucket_volumes = get_reporter_volumes_metadata(R2_UNREDACTED_BUCKET, reporter)
        static_bucket_volumes = get_reporter_volumes_metadata(R2_STATIC_BUCKET, reporter)

        if not unredacted_bucket_volumes:
            raise Exception(f"Did not find any reporter volumes in {R2_UNREDACTED_BUCKET} bucket")

        if not static_bucket_volumes:
            raise Exception(f"Did not find any reporter volumes in {R2_STATIC_BUCKET} bucket")

        return map_files_for_unredaction(json.loads(static_bucket_volumes), json.loads(unredacted_bucket_volumes))

    if publication_year:
        static_bucket_volumes = json.loads(get_volumes_metadata(R2_STATIC_BUCKET))
        unredacted_bucket_volumes = json.loads(get_volumes_metadata(R2_UNREDACTED_BUCKET))

        vols_published_before = list(filter(
            lambda item: item.get('publication_year') is not None and item['publication_year'] < int(publication_year),
            static_bucket_volumes)
        )

        return map_files_for_unredaction(vols_published_before, unredacted_bucket_volumes)


def map_files_for_unredaction(static_volumes, unredacted_volumes):
    """
    Skips volumes that are already flagged as `unredacted`
    Returns the ids of volumes that need to be unredacted
    Returns a list of files that need replacing in static bucket
    """
    volumes_to_unredact = []
    files = []

    for volume in static_volumes:
        if not volume["redacted"]:
            continue

        if volume["id"] in [unredacted_vol["id"] for unredacted_vol in unredacted_volumes]:
            volumes_to_unredact.append({
                "reporter": volume["reporter_slug"],
                "volume_folder": volume["volume_folder"]
            })
            files.extend(get_unredacted_volume_files(volume))

    return volumes_to_unredact, files


def get_unredacted_volume_files(volume):
    """
    Returns a list of dictionaries with volume file source and destination paths
    """
    key_prefix = f"{volume['reporter_slug']}/{volume['volume_folder']}"
    extensions = ["pdf", "zip", "tar", "tar.csv", "tar.sha256"]
    volume_files = []

    # grab the volume artifacts
    for page in r2_paginator.paginate(Bucket=R2_UNREDACTED_BUCKET, Prefix=f"{key_prefix}.",
                                      PaginationConfig={"PageSize": 1000}):
        for item in page["Contents"]:
            if any(ext in item["Key"] for ext in extensions):
                volume_files.append(
                    {
                        "source": f"{RCLONE_R2_UNREDACTED_BASE_URL}{item['Key']}",
                        "destination": f"{RCLONE_R2_CAP_STATIC_BASE_URL}{item['Key']}",
                    }
                )

    # grab the volume case and metadata files
    for page in r2_paginator.paginate(Bucket=R2_UNREDACTED_BUCKET, Prefix=f"{key_prefix}/",
                                      PaginationConfig={"PageSize": 1000}):
        for item in page["Contents"]:
            volume_files.append(
                {
                    "source": f"{RCLONE_R2_UNREDACTED_BASE_URL}{item['Key']}",
                    "destination": f"{RCLONE_R2_CAP_STATIC_BASE_URL}{item['Key']}",
                }
            )

    return volume_files

