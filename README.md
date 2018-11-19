

TL;DR:

>One of the goals of Apache Arrow is to serve as a common data layer enabling zero-copy data exchange between multiple frameworks. A key component of this vision is the use of off-heap memory management (via Plasma) for storing and sharing Arrow-serialized objects between applications.

## Python examples

```
from plasmar import DataFrameClient
import pandas as pd
import numpy as np

# Create a test dataframe
someDF = pd.DataFrame( np.random.rand(100,3), columns = ['a', 'b', 'c'])

# Create a DF client
client = DataFrameClient("/tmp/plasma")

# Write the dataframe
client.write_df(someDF, "someDF")
```


## References

1. https://arrow.apache.org/blog/2017/08/08/plasma-in-memory-object-store/
2. 2. 