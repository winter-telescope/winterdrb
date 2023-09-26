"""
Apply basic cleaning cuts to a dataframe of detections
"""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def apply_clean_filter(detections: pd.DataFrame) -> pd.DataFrame:
    """
    Apply basic cleaning cuts to a dataframe of detections

    :param detections: All detections
    :return: Cleaned detections
    """
    masks = [
        detections["nbad"] < 2,
        detections["chipsf"] < 3.0,
        detections["sumrat"] > 0.7,
        detections["fwhm"] < 10.0,
        detections["magdiff"] < 1.6,
        detections["magdiff"] > -1.0,
        detections["mindtoedge"] > 50.0,
    ]
    mask = np.ones(len(detections), dtype=bool)
    for new_mask in masks:
        mask *= new_mask

    logger.info(f"{np.sum(mask)} / {len(mask)} detections pass all cuts")
    return detections[mask]
