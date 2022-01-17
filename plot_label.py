import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

df = pd.read_csv("data.csv", quoting=1, encoding="utf-8", sep="~")

x = df['category'].value_counts()
print(x)
# sns.barplot(x.index, x)
# ax = sns.barplot(x=x.index, y=x)
#
# plt.show()