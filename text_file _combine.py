import pandas as pd
import os


path ="/media/anamika/DATA/WPI_COURSES/UMASS/SDOH-Project/Data/Track2_SubtaskA/Track2_SubtaskA/Annotations/dev"
df_text = pd.DataFrame(columns = ["text_id", "text"])
for file in os.listdir(path + "/mimic"):
    if file.endswith("txt"):
        print(file)
        text_id = file.split(".")[0]
        with open(path+'/mimic/'+file, 'r') as file:
            data = file.read()
        df_text.loc[len(df_text)] = [text_id, data]

df_text.to_csv(path+"/dev_text_combine.csv", sep = "|", index = False)
