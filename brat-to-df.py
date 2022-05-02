import pandas as pd
import re
import os
from natsort import natsorted,index_natsorted,order_by_index,natsort_keygen
# path = "/media/anamika/DATA/WPI_COURSES/UMASS/SDOH-Project/Data/Track2_SubtaskA/Track2_SubtaskA/Annotations/train"

path ="/media/anamika/DATA/WPI_COURSES/UMASS/SDOH-Project/Data/Track2_SubtaskA/Track2_SubtaskA/Annotations/dev"

df_ner_entities = pd.DataFrame(columns = ["file_id","standoff_id", "entity", "begin", "end", "chunk"])
df_relationship = pd.DataFrame(columns=["file_id","relation_tag", "entity1_standoff_id", 'relation', 'entity1', 'entity1_begin',
                                        'entity1_end', 'chunk1', "entity2_standoff_id", 'entity2',
                                        'entity2_begin','entity2_end','chunk2'])

for file in os.listdir(path+"/mimic"):
    print("file:", file)
    if file.endswith("ann"):
        text_id = file.strip(".ann")
        df_ner_temp = pd.DataFrame(columns=["file_id", "standoff_id", "entity", "begin", "end", "chunk"])
        df_rel_temp = pd.DataFrame(columns=["file_id", "relation_tag", "entity1_standoff_id", 'relation', 'entity1', 'entity1_begin',
                     'entity1_end', 'chunk1', "entity2_standoff_id", 'entity2',
                     'entity2_begin', 'entity2_end', 'chunk2'])
        with open(path+"/mimic/"+file, 'r') as ann_file:
            lines = ann_file.readlines()
            if len(lines) > 0:
                lines = natsorted(lines, reverse=True)
                for line in lines:
                    standoff_line = line.split(sep="\t")
                    if standoff_line[0][0] == 'T':
                        standoff_id = standoff_line[0]
                        entity_ls = standoff_line[1].split()
                        regex = re.compile(r'^[0-9]+;[0-9]+')
                        entity_ls_new = [i for i in entity_ls if not regex.match(i)]
                        entity = entity_ls_new[0]
                        begin = entity_ls_new [1]
                        end = entity_ls_new[2]
                        chunk = standoff_line[-1].strip('\n')
                        df_ner_temp.loc[len(df_ner_temp)] = [text_id, standoff_id, entity, begin, end, chunk]

                    df_ner_temp.sort_values(by="standoff_id", key=natsort_keygen(), ignore_index=True, inplace=True)

                    if standoff_line[0][0] == 'E':
                        re_list = standoff_line[1].strip('\n').strip(" ").split(" ")
                        for i in range(1, len(re_list)):
                            tag = standoff_line[0]
                            ent_filter_1 = re_list[0].split(":")[-1]
                            entity1_standoff_id = df_ner_temp.loc[(df_ner_temp.file_id == text_id) & (df_ner_temp.standoff_id == ent_filter_1)]["standoff_id"].item()
                            entity1 = df_ner_temp.loc[(df_ner_temp.file_id == text_id) & (df_ner_temp.standoff_id == ent_filter_1)]["entity"].item()
                            entity1_begin = df_ner_temp.loc[(df_ner_temp.file_id == text_id) & (df_ner_temp.standoff_id == ent_filter_1)]["begin"].item()
                            entity1_end = df_ner_temp.loc[(df_ner_temp.file_id == text_id) & (df_ner_temp.standoff_id == ent_filter_1)]["end"].item()
                            chunk1 = df_ner_temp.loc[(df_ner_temp.file_id == text_id) & (df_ner_temp.standoff_id == ent_filter_1)]["chunk"].item()

                            relation = re_list[i].split(":")[0]
                            ent_filter_2 = re_list[i].split(":")[-1].strip("\n")
                            entity2_standoff_id = df_ner_temp.loc[(df_ner_temp.file_id == text_id) & (df_ner_temp.standoff_id == ent_filter_2)]["standoff_id"].item()
                            entity2 = df_ner_temp.loc[(df_ner_temp.file_id == text_id) & (df_ner_temp.standoff_id == ent_filter_2)]["entity"].item()
                            entity2_begin = df_ner_temp.loc[(df_ner_temp.file_id == text_id) & (df_ner_temp.standoff_id == ent_filter_2)]["begin"].item()
                            entity2_end = df_ner_temp.loc[(df_ner_temp.file_id == text_id) & (df_ner_temp.standoff_id == ent_filter_2)]["end"].item()
                            chunk2 = df_ner_temp.loc[(df_ner_temp.file_id == text_id) & (df_ner_temp.standoff_id == ent_filter_2)]["chunk"].item()
                            df_rel_temp.loc[len(df_rel_temp)] = [text_id, tag,entity1_standoff_id, relation, entity1,
                                                                         entity1_begin,entity1_end, chunk1, entity2_standoff_id, entity2,
                                                                         entity2_begin, entity2_end, chunk2]
                    df_rel_temp.sort_values(by="relation_tag", key=natsort_keygen(), ignore_index=True, inplace=True)

            print("dataframe appended")
            print("Before", len(df_ner_entities))
            print("Before", len(df_relationship))
            if len(df_ner_temp) > 0:
                df_ner_entities = pd.concat([df_ner_entities, df_ner_temp], ignore_index= True)
                df_relationship = pd.concat([df_relationship, df_rel_temp], ignore_index=True)
            print("After", len(df_ner_entities))
            print("After", len(df_relationship))






df_ner_entities.to_csv(path +"/dev_combine_ner.csv", sep= "|", index =False)
df_relationship.to_csv(path +"/train_combine_relation.csv", sep= "|", index = False)
