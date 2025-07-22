import sys

import pandas as pd

# get passed system argruments
print(sys.argv)

# get the passed second system argrument
# ex. docker run -it test:v1 2025-07-21
# - argrument 0 = image name
# - argrument 1 = 2025-07-21
day = sys.argv[1]

# some fancy stuff with pandas

print('job finished successfully for day = ' + day)