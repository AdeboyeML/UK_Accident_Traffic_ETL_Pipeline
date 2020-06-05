from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.DownloadfromS3Operator,
        operators.LoadToRedshiftOperator,
        operators.UploadToS3Operator,
        operators.DataQualityOperator
    ]
    helpers = [
    ]
