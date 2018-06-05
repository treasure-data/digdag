Release 0.9.14
==============

Operator Changes
----------------

* Docker command executor (``docker: {image: name}``) supports ``always_pull: true`` option (@alu++).

* ``emr>`` operator supports region options and secrets. See documents for details. (@saulius++)


General Changes
---------------

* Digdag validates _error task before workflow starts (with local mode) or when a workflow is uploaded (with server mode). It was validated only when _error task actually ran but now it's easier for us to notice broken workflows.

* Fixed uncaught exception in workflow executor loop when _error tasks had a problem. This is only for being more defensive because broken _error tasks shouldn't exist thanks to above another fix.


Release Date
------------
2017-08-11

Contributors
------------------
* Mitsuhiro Koga
* Mitsunori Komatsu
* Sadayuki Furuhashi
* Saulius Grigaliunas
* Takehiro Shiozaki
* Toru Takahashi
* alu
* parroty

