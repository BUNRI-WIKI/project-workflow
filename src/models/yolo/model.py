# from datetime import datetime

# NOW_TIME = datetime.now().strftime('%y%m%d')

# class YoloModel:
#     def __init__(
#         self, 
#         class_path: str,
#         param_path: str,
#     ) -> None:
#         import yaml
#         from ultralytics import YOLO, settings

#         self.class_path = class_path
        
#         # load the configuration file 
#         with open(param_path) as f:
#             self.params = yaml.safe_load(f)["yolo"]

#         # Load a pretrained YOLO model
#         self.model = YOLO(self.params["model"])

#         settings.update({
#             "runs_dir": self.params["runs_dir"],
#             "mlflow": False,
#             "tensorboard": True,
#         })
        
#     def train(self):
#         # Train the model using the 'recycle.yaml' dataset for n epochs
#         self.model.train(
#             data=self.class_path,
#             epochs=self.params["epochs"],
#             patience=self.params["patience"],
#             batch=self.params["batch"],
#             imgsz=self.params["imgsz"],
#             cache=self.params["cache"],
#             device=self.params["device"],
#             workers=self.params["workers"],
#             name=self.params["name"]+f"_{NOW_TIME}",
#             pretrained=self.params["pretrained"],
#             optimizer=self.params["optimizer"],
#             verbose=self.params["verbose"],
#             lr0=self.params["lr0"],
#             momentum=self.params["momentum"],
#             weight_decay=self.params["weight_decay"],
#             box=self.params["box"],
#             cls=self.params["cls"],
#             dropout=self.params["dropout"],
#             val=self.params["val"],
#             project=self.params["save_dir"]+f"{NOW_TIME}"
#         )

#         # Evaluate the model's performance on the validation set
#         self.model.val()

#         import torch
#         torch.save(self.model.state_dict(), self.params["save_state_dict_dir"]+f"{NOW_TIME}.pt")


# src/models/yolo/model.py

from pathlib import Path
from datetime import datetime
import torch
from ultralytics import YOLO, settings
import yaml

ROOT_DIR = Path(__file__).resolve().parents[2]
NOW_TIME = datetime.now().strftime("%y%m%d")

class YoloModel:
    def __init__(self, class_path: Path, param_path: Path) -> None:
        """
        YOLO 모델 초기화
        """
        self.class_path = class_path
        self.param_path = param_path

        # 하이퍼파라미터 로딩
        with open(param_path) as f:
            self.params = yaml.safe_load(f)["yolo"]

        # 사전학습 모델 로딩
        self.model = YOLO(self.params["model"])

        # 설정 (TensorBoard 로깅 활성화)
        settings.update({
            "runs_dir": str(self.params["runs_dir"]),
            "mlflow": False,
            "tensorboard": True,
        })

    def train(self):
        """
        YOLO 모델 학습
        """
        self.model.train(
            data=str(self.class_path),
            epochs=self.params["epochs"],
            patience=self.params["patience"],
            batch=self.params["batch"],
            imgsz=self.params["imgsz"],
            cache=self.params["cache"],
            device=self.params["device"],
            workers=self.params["workers"],
            name=self.params["name"] + f"_{NOW_TIME}",
            pretrained=self.params["pretrained"],
            optimizer=self.params["optimizer"],
            verbose=self.params["verbose"],
            lr0=self.params["lr0"],
            momentum=self.params["momentum"],
            weight_decay=self.params["weight_decay"],
            box=self.params["box"],
            cls=self.params["cls"],
            dropout=self.params["dropout"],
            val=self.params["val"],
            project=str(self.params["save_dir"]) + f"{NOW_TIME}"
        )

        # 검증
        self.model.val()

        # state_dict 저장
        output_dir = Path(self.params["save_state_dict_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        torch.save(self.model.state_dict(), output_dir / f"{NOW_TIME}.pt")

        print(f"[INFO] YOLO model trained and saved at {output_dir / f'{NOW_TIME}.pt'}")

