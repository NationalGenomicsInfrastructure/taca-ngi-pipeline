Project and Sample delivery
===========================

TACA has a delivery module that assists in preparing and transferring data for
a project, to a specified destination. Before starting the delivery process of 
a sample, the status of the sample is fetched from the tracking database. If
the sample does not have an analysis status that indicates that the analysis is
complete, the sample will be skipped. Likewise, if the sample has a delivery 
status indicating that it has already been delivered, it will be skipped. This
makes it safe to run the delivery script repeatedly for projects and samples,
any sample that should not be delivered will be skipped. 

If a non-recoverable error occurs during the delivery of a sample, an email will
be sent to the address specified in the configuration or on the command line 
(see below), and the delivery will skip to the next sample. Errors which are 
considered to be recoverable (e.g. individual files missing for a sample) will
generate a warning in the log but will not trigger an email notification or
abort the delivery of the current sample. **It is therefore important to review
the log file after delivery**.

After successful delivery of a sample or project, the delivery status in the
tracking database will be updated. Failed or aborted deliveries will not result
in an updated delivery status.

The delivery is broken down into two steps: **staging** and **transfer**.

Staging
-------

In the staging step, files that are to be tranferred are located and symlinked 
into a staging folder, according to the TACA configuration. A checksum is calculated for each staged file. 

Transfer
--------

In the transfer step, the previously staged folder is transferred to the 
destination location, specified in the configuration using rsync. It is possible
to transfer across hosts but the authentication has to be taken care of outside
of TACA, e.g. with key authorization or some ssh-agent solution. If transfer 
was done on a local system, the integrity of the transferred files is verified
by comparing to the checksums calculated in the staging folder.

Configuration
-------------

Configuration options affecting the delivery are specified under the 
``deliver:`` section in the TACA configuration. Many configuration options
can also be given on the command line. Option values given on the command line 
takes precedence over option values specified in the configuration. 

It is possible to use placeholders in paths in the configuration file. The
placeholders will be evaluated and replaced with the corresponding value at 
run-time, which is useful for specifying e.g. sample-specific paths. The syntax
for a placeholder is the name in capital letters, surrounded by underscores, 
e.g. ``_SAMPLEID_``. The placeholder will be replaced with the corresponding
configuration option in lowercase letters, i.e. in the example above, this will 
be ``sampleid``.

Below is a description of common configuration options.

``operator`` an email address to send notifications of errors occurring during
delivery to

``stagingpath`` path where files will be staged. *Required*

``deliverypath`` path where the staged files will be delivered. If used together
with option ``remote_host``, this will be the path on the remote system. 
*Required*

``files_to_deliver`` a list of tuples, where the first entry is a path 
expression (this can be a file glob), pointing to a file or folder that should 
be delivered, and the second entry is the path to where the matching file(s) or 
folders (and contents) will be staged by symlinking. *Required*

``hash_algorithm`` the algorithm that should be used for calculating the file
checksums. Accepted values are algorithms available through the Python `hashlib`_ module.

Below is a sample configuration snippet:

.. code-block:: yaml

    deliver:
        deliverypath: /local/or/remote/path/to/transfer/destination
        stagingpath: /path/to/stage
        operator: notify-on-error@email.address
        files_to_deliver:
            -
                - /expression/to/source/file/or/folder
                - _STAGINGPATH_/the/file/or/folder/and/contents
            -
                - /expression/can/be/a/*/glob/as/well*
                - _STAGINGPATH_/files/or/folders/matching/glob

In addition, there is a dependency on the `ngi_pipeline`_ module, which requires
its own configuration file. Please refer to the ngi_pipeline `documentation`_ 
for details.

Command line options
--------------------

The delivery script can be run for an entire project or for one or more samples
in a project. The main delivery command is ``taca deliver`` and it takes further
subcommands as described below. The deliver command accepts a number of options,
run ``taca deliver --help`` for a description of these.

Project delivery
~~~~~~~~~~~~~~~~

Running the delivery script for a project is equivalent to delivering all 
samples in the project. To launch a delivery for a project, use the command 
``taca deliver project``. Typically, the command takes one positional argument, 
which is the name of the project to deliver, e.g. 
``taca deliver project MH-0336``

For a full listing of available options, run the ``taca deliver project --help``

Sample delivery
~~~~~~~~~~~~~~~

One or more samples in a project can be delivered with the ``taca deliver 
sample`` command. The command takes any number of positional arguments, where 
the first is expected to be the project name and the following arguments are
assumed to be the sample names, e.g. 
``taca deliver sample MH-0336 Sample1 Sample 3 Sample 5``

For a full listing of available options, run the ``taca deliver sample --help``
 
Example usage
-------------

Deliver all finished samples belonging to project MH-0336 according to the 
configuration in ``conf/taca_cfg.yaml``:

``taca -c conf/taca_cfg.yaml deliver project MH-0336``

Deliver the specified samples belonging to the project according to the 
default configuration:

``taca deliver sample MH-0336 Sample1 Sample 3 Sample 5``

.. _hashlib: https://docs.python.org/2/library/hashlib.html

.. _ngi_pipeline: https://github.com/NationalGenomicsInfrastructure/ngi_pipeline

.. _documentation: https://github.com/NationalGenomicsInfrastructure/ngi_pipeline

