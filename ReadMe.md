# WikiData
This snippet exemplifies how to extract data from WikiData. A simple class is used to manage the different attributes 
and methods are associated to the calls. A special emphasis on fast execution is done by forcefully requesting the data.
The script uses Joblib to parallelize the calls. Furthermore, the transient nature of issues made it necessary to 
manage state of the calls. The calls are requested per batch. Failed calls will be pickled. The failed calls will be 
re-requested until success. The results of the calls are stored in `temp` folder. The output is a clean pandas 
DataFrame.

This was a small fun-project, feel free to use it any way you want. I take no responsibility or liability over the 
contents of this
repository. 

[Sphinx-Documentation](docs/_build/html/index.html) can be found here, but is still very much `in progress`