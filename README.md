Kinetica Python API
===================

This is the 6.2.0 version of the client-side Python API for Kinetica.

To install this package, run 'sudo setup.py install' in the root directory of
the repo.

There is also an example file in the example directory.

The documentation can be found at http://www.kinetica.com/docs/6.2/index.html.  
The python specific documentation can be found at:

*   http://www.kinetica.com/docs/6.2/tutorials/python_guide.html
*   http://www.kinetica.com/docs/6.2/api/python/index.html


For changes to the client-side API, please refer to CHANGELOG.md.  For
changes to GPUdb functions, please refere to CHANGELOG-FUNCTIONS.md.


Troubleshooting

* If you get an error when running pip like

```
  "Traceback ... File "/bin/pip", line 5, in <module> from pkg_resources import load_entry_point"
```

please try upgrading pip with command:

```
    python -m pip install --upgrade --force pip
```
 
* If you get an error when running pip like
```
    "Exception: Traceback ... File "/usr/lib/python2.7/site-packages/pip/basecommand.py", line 215, in main status = self.run(options, args)"
```

please try downgrading your version of pip setuptools with command:

```
    pip install setuptools==33.1.1
```
