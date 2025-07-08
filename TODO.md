# Incomplete and malformed logs

The logger is supposed to be robust and flexible for most [recommended usages of Python on Polaris](https://docs.alcf.anl.gov/polaris/data-science/python/) that are based on the shared Anaconda modules (and eventually Aurora; and formerly ThetaKNL and ThetaGPU). For example, it should also log modules installed on a virtual environment that extends the base conda environments via `--system-site-packages`.

The script is also supposed to implicitly capture all PBS environment variables via `env': os.environ.copy(),` for any usage of the conda modules on a compute node, interactive or batch job.
E.g. in `/lus/eagle/logs/pythonlogging/module_usage/2025/04/03/x3004c0s13b0n0.237323.16.21.10.421970` on Polaris, you can see:
```text
 "PBS_JOBID": "4096686.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
 ```

## March 2025 examples on Polaris

- 5914 logfiles
  - 5787 non-empty logfiles
  - 2613 compute node logfiles
	- 105 are 0-bytes, see below
	- 2508 non-empty compute node logfiles
		- 2490 have `SLURM*` environment variables in them somewhere (e.g. `"SLURM_MPI_TYPE": "cray_shasta"`, which doesn't make much sense on this HPCM machine)
		
Of the 2613 compute node logfiles:		
- 138 compute node logfiles are missing PBS environment variables like `PBS_JOBID`
  - But only 33 are **nonempty** compute node logfiles that are missing PBS. All are from a single user `awikner`, and they mention VSCode
  - [ ] Figure out what VSCode integration is breaking them


The empty logfiles are likely unavoidable, given how the logger works on `atexit()`:
```
PyModuleSnooper: log loaded module paths on CPython shutdown

PyRats relies on AST and parsing the .py file *before* interpreter begins
execution.  This precludes catching dynamic imports and does not *follow*
dependencies.

This approach uses atexit() to inspect sys.modules and log all loaded modules
upon interpreter shutdown.  It should log under most normal termination
circumstances. Atexit() should be preferred over registering signal handlers,
because users may register their own handlers.

* This will run on SIGINT (ctrl+c)
  or on any non-fatal Python exceptions (SyntaxError, ValueError, etc...)
* It will NOT run for unhandled signals (SIGTERM, SIGKILL)
* It will NOT run if os._exit() is invoked directly
* It will NOT run if CPython interpreter itself crashes
* Refer to https://docs.python.org/3.6/library/atexit.html
```
