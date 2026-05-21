import argparse

from .CMS_config import PART_D_DATASETS


def download_year(yr):
    print(f"{yr}:{PART_D_DATASETS[yr]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--years",
        "-y",
        help="specify the years you want to download the CMS Part D provider data",
        type=int,
        nargs="+",
        required=True,
    )
    args = parser.parse_args()

    for year in args.years:
        download_year(year)
