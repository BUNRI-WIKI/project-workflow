
# class dataset:
#     def __init__(self):
#         CLASS_DICT = [
#             "금속캔", "종이", "페트병", "플라스틱", "스티로폼", "비닐", "유리병", "건전지", "형광등"
#         ]
#         self.class_dict = {c:i for i, c in enumerate(CLASS_DICT)}


#     '''
#     Box Normalization
#         ANNOTATION_INFO["POINTS"] format    [topleftX, topleftY, w, h]
#         YOLO format                         [label, centerX, centerY, w, h]
#     '''
#     def BoxNormalization(self, image_info, annotation):
#         image_height = image_info['IMAGE_HEIGHT']  
#         image_width = image_info['IMAGE_WIDTH']
#         x, y, w, h = annotation['POINTS'][0]

#         nx = (x + w / 2) / image_width
#         ny = (y + h / 2) / image_height
#         nw = w / image_width
#         nh = h / image_height

#         return [nx, ny, nw, nh]
    

#     '''
#     Segement Normalization
#         ANNOTATION_INFO["POINTS"] format    [[x1, y1], [x2, y2], ..., [xn yn]]
#         YOLO format                         [label, x1, y1, x2, y2, ..., xn yn]
#     '''
#     def SegementNormalization(self, image_info, annotation):
#         image_height = image_info['IMAGE_HEIGHT']  
#         image_width = image_info['IMAGE_WIDTH']
#         segments = annotation['POINTS']
#         result = []

#         for segment in segments:
#             x, y = segment
#             nx = x / image_width
#             ny = y / image_height
#             result.append(nx)
#             result.append(ny)

#         return result


#     def Normalization(self, image_info, annotation):
#         if   annotation['SHAPE_TYPE'] == 'BOX':     return self.BoxNormalization(image_info, annotation)
#         elif annotation['SHAPE_TYPE'] == 'POLYGON': return self.SegementNormalization(image_info, annotation)
#         else:
#             raise


#     def makeYOLOLabels(self, image_info, annotations):
#         return [
#             "{} {}".format(
#                 self.class_dict[annotation['CLASS']],
#                 ' '.join(str(p) for p in self.Normalization(image_info, annotation))
#             ) for annotation in annotations
#         ] 


#     def preprocess(
#         self,
#     ) -> None:
#         import os
#         import json

#         train_dirs = [
#             os.environ["YOLO_LABEL__TRAIN_SRC_DIR"], 
#             os.environ["YOLO_LABEL__TRAIN_DST_DIR"],
#         ]
#         valid_dirs = [
#             os.environ["YOLO_LABEL__VALID_SRC_DIR"],
#             os.environ["YOLO_LABEL__VALID_DST_DIR"]
#         ]

#         for src, dst in [train_dirs, valid_dirs]:

#             # load folder
#             for file in [f for f in os.listdir(src)]:

#                 # opne json file
#                 with open(os.path.join(src, file), encoding='UTF8') as f:
#                     data = json.load(f)
#                     image_info = data['IMAGE_INFO']
#                     image_annotations = data['ANNOTATION_INFO']

#                     yolo_labels = self.makeYOLOLabels(image_info, image_annotations)

#                     yolo_file_name = image_info['FILE_NAME'].split(".")[0]
#                     yolo_file_path = os.path.join(dst, yolo_file_name + ".txt")

#                     with open(yolo_file_path, 'w') as yolo_file:
#                         yolo_file.write('\n'.join(yolo_labels))
                
#             print("The json file was converted to a yolo file and saved in the {}".format(src))


# src/models/yolo/dataset.py

import os
import json
from pathlib import Path

class YoloDatasetPreprocessor:
    def __init__(self, class_list: list):
        self.class_dict = {cls_name: idx for idx, cls_name in enumerate(class_list)}

    def box_normalization(self, image_info, annotation):
        height = image_info["IMAGE_HEIGHT"]
        width = image_info["IMAGE_WIDTH"]
        x, y, w, h = annotation["POINTS"][0]

        return [
            (x + w / 2) / width,  # center_x
            (y + h / 2) / height, # center_y
            w / width,            # width
            h / height            # height
        ]

    def segment_normalization(self, image_info, annotation):
        height = image_info["IMAGE_HEIGHT"]
        width = image_info["IMAGE_WIDTH"]
        result = []
        for point in annotation["POINTS"]:
            nx = point[0] / width
            ny = point[1] / height
            result.extend([nx, ny])
        return result

    def normalize_annotation(self, image_info, annotation):
        if annotation["SHAPE_TYPE"] == "BOX":
            return self.box_normalization(image_info, annotation)
        elif annotation["SHAPE_TYPE"] == "POLYGON":
            return self.segment_normalization(image_info, annotation)
        else:
            raise ValueError(f"Unsupported SHAPE_TYPE: {annotation['SHAPE_TYPE']}")

    def make_yolo_labels(self, image_info, annotations):
        labels = []
        for ann in annotations:
            class_id = self.class_dict.get(ann["CLASS"])
            if class_id is not None:
                normalized = self.normalize_annotation(image_info, ann)
                labels.append(f"{class_id} " + " ".join(str(x) for x in normalized))
        return labels

    def preprocess_folder(self, src_dir: Path, dst_dir: Path):
        src_dir = Path(src_dir)
        dst_dir = Path(dst_dir)
        dst_dir.mkdir(parents=True, exist_ok=True)

        json_files = list(src_dir.glob("*.json"))
        print(f"[INFO] Found {len(json_files)} annotation files in {src_dir}")

        for json_file in json_files:
            with open(json_file, encoding="utf-8") as f:
                data = json.load(f)
                image_info = data["IMAGE_INFO"]
                annotations = data["ANNOTATION_INFO"]

            yolo_labels = self.make_yolo_labels(image_info, annotations)

            label_filename = dst_dir / (Path(image_info["FILE_NAME"]).stem + ".txt")
            with open(label_filename, "w") as f:
                f.write("\n".join(yolo_labels))

        print(f"[INFO] Preprocessing completed for {src_dir} -> {dst_dir}")

