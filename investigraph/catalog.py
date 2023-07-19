from datetime import datetime
from typing import Any, Dict, Generator, Optional, Union

import requests
import yaml
from nomenklatura.dataset import DataCatalog
from nomenklatura.util import datetime_iso
from zavod.dataset import ZavodDataset

from investigraph.util import PathLike

from .logging import get_logger

log = get_logger(__name__)


def flatten_catalog(url: str) -> Generator[Dict[str, Any], None, None]:
    try:
        resp = requests.get(url)
        catalog_data = resp.json()
        for dataset in catalog_data.get("datasets", []):
            if dataset.get("type") != "collection":
                yield dataset
    except Exception as exc:
        log.error(str(exc), url=url)


def build_catalog(catalog_in: PathLike) -> DataCatalog:
    """
    build a catalog based on yaml input:

    ```yaml
    datasets:
      - include: https://data.ftm.store/investigraph/ec_meetings/index.json
    - - include_catalog: https://data.ftm.store/catalog.json
      - include_catalog:
          url: https://data.opensanctions.org/datasets/latest/index.json
          exclude:
            - validation
          maintainer:
            name: OpenSanctions
            url: https://opensanctions.org
            description: |
              OpenSanctions is an international database of persons and companies
              of political, criminal, or economic interest. The project combines
              the sanctions lists, databases of politically exposed persons, and
              other information about persons in the public interest into a single,
              easy-to-use dataset.

    ```

    """
    log.info("building catalog", catalog=str(catalog_in))
    seen = set()
    with open(catalog_in) as fh:
        catalog_in_data = yaml.safe_load(fh)
    catalog_in = str(catalog_in)  # for logging
    catalog = DataCatalog(ZavodDataset, {})
    catalog.updated_at = datetime_iso(datetime.utcnow())
    for ds_data in catalog_in_data["datasets"]:
        include_url: Optional[str] = ds_data.pop("include", None)
        if include_url is not None:
            try:
                resp = requests.get(include_url)
                ds_data = resp.json()
                name = ds_data["name"]
                if name not in seen:
                    ds = catalog.make_dataset(ds_data)
                    log.info("Dataset: %r" % ds, catalog=catalog_in)
                    seen.add(name)
                else:
                    log.warn(
                        "Dataset `%s` already in catalog." % name, catalog=catalog_in
                    )
            except Exception as exc:
                log.error(str(exc), catalog=catalog_in, url=include_url)
        include_catalog: Optional[Union[str, Dict[str, Any]]] = ds_data.pop(
            "include_catalog", None
        )
        if include_catalog is not None:
            if isinstance(include_catalog, str):
                include_catalog_url = include_catalog
                exclude = []
            else:
                include_catalog_url = include_catalog.pop("url")
                exclude = include_catalog.pop("exclude", [])

            for ds_data in flatten_catalog(include_catalog_url):
                name = ds_data["name"]
                if name not in seen and name not in exclude:
                    ds = catalog.make_dataset(ds_data)
                    log.info("Dataset: %r" % ds, catalog=catalog_in)
                    seen.add(name)
                else:
                    log.warn(
                        "Dataset `%s` already in catalog." % name, catalog=catalog_in
                    )

    return catalog
