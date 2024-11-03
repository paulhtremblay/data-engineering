Problem
========

This project illustrates a potential bug with Google Clould Secretmanager package.
The project needs to access a secret by calling the `google.cloud import secretmanager`.
The library is installed in the standard way, by creating a requirements file, and 
then pip installing these requirements on the Docker image.

The main pipeline is `secret_maganger_ex.py`. It calls the `google.cloud import secretmanager`
twice. The first time is with the launcher image. The second time is in a Pardo class.

With the launcher, the code executes successfully. But the worker image
prodces this error:

```
ImportError: cannot import name 'secretmanager' from 'google.cloud' (unknown location)
```

So the launcher can find the library, but the worker cannot. 

To make my Proof of Concept complete, I imported another library, `requests`. The top of my lib1 looks like:
 ```
import logging
import requests
from google.cloud import secretmanager
```

The lib1 can import requests (an extenal libary not natively there). It does not fail when 
importing `requests`. If I just import requests, my job would run. This indicates that there 
is something different and non-standard when importing `google.cloud import secretmanager`.

Solution
========

The solution is to put the following in `setup.py`:

```
    install_requires=['google-cloud-secret-manager==2.21.0'],
```

There are two setup files in this project. One is `setup_will_not_work.py`. Note that this
file does not contain the above line. If this file is renamed `setup.py` and used to build the 
Docker image, the job will fail.

If I use `setup.py` with the `install_requires`, the job will succeed. 
