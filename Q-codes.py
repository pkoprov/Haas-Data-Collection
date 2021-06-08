import pandas as pd

all_macros = pd.read_excel("../Book of Macros.xlsx", engine="openpyxl")

columns = ["Variable", "Description"]
codes = pd.DataFrame(columns=columns)

for n, i in enumerate(all_macros["NGC Variable"]):

    i = str(i)
    i = i.split("-")

    if len(i)>1:
        i1 = int(i[0])
        i2 = int(i[1])
        for m, j in enumerate(range(i1,i2+1)):
            code = pd.DataFrame([[f"?Q600 {j}", f"{all_macros['Usage'][n]} {m+1} (Var {j})"]], columns=columns)
            codes = codes.append(code, ignore_index=True)
            # print(all_macros["Usage"][n], m+1)
            # print(j)

    else:
        i = int(i[0])
        code = pd.DataFrame([[f"?Q600 {i}", all_macros["Usage"][n]]], columns=columns)
        codes = codes.append(code, ignore_index=True)
        # print(i)
    print(codes.tail())

codes.to_excel("Q-codes.xlsx", index=False)
