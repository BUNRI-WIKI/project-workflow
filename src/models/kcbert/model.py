from pathlib import Path
from datetime import datetime

ROOT_DIR = Path(__file__).resolve().parents[1]
NOW_TIME = datetime.now().strftime('%y%m%d')

import torch
import transformers

class KcbertModel():
    def __init__(
        self,
        param_path: str,
    ) -> None:
        import yaml

        # load the configuration file 
        with open(param_path) as f:
            self.params = yaml.safe_load(f)["kcbert"]
        
        from torch.utils.tensorboard import SummaryWriter
        self.writer = SummaryWriter(
            log_dir="s3://midas-bucket-1/models/kcbert/",
            filename_suffix=f"kcbert_{NOW_TIME}"
        )

        self.model = transformers.AutoModelForSequenceClassification.from_pretrained(
            self.params["base_model_name"], num_labels=11
        )
        self.model.load_state_dict(torch.load(self.params["model_param_save_path"]))
        self.tokenizer = transformers.AutoTokenizer.from_pretrained(self.params["base_model_name"])

        # Set device for model
        self.device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        self.model = self.model.to(self.device)

    def train(self):
        learning_rate=float(self.params["learning_rate"])
        batch_size=self.params["batch_size"]
        epochs=self.params["epochs"]

        from models.kcbert.dataset import CustomHateSpeechDataset
        dataset = CustomHateSpeechDataset(
            tokenizer=self.tokenizer, 
            max_length=128
        )

        # Split dataset into training and validation
        from sklearn.model_selection import train_test_split
        train, valid = train_test_split(
            dataset,
            test_size=self.params["test_size"], 
            random_state=self.params["random_state"]
        )

        #
        from torch.utils.data import DataLoader
        train_dataloader = DataLoader(train, batch_size=batch_size, shuffle=True)
        valid_dataloader = DataLoader(valid, batch_size=batch_size, shuffle=True)

        
        optimizer = torch.optim.AdamW(self.model.parameters(), lr=learning_rate)
        criterion = torch.nn.CrossEntropyLoss()

        for epoch in range(epochs):
            batch_train_loss, batch_valid_loss = [], []
            total = correct = 0

            self.model.train()
            for i, batch in enumerate(train_dataloader):
                labels = batch["label"].to(self.device)
                input_ids = batch["input_ids"].to(self.device)
                attention_mask = batch["attention_mask"].to(self.device)
                
                optimizer.zero_grad()
                outputs = self.model(input_ids, attention_mask=attention_mask)
                logits = outputs.logits
                loss = criterion(logits, labels)
                loss.backward()
                optimizer.step()

                train_preds = logits.argmax(dim=1) 
                correct += (train_preds == labels).sum().item()
                total += labels.size(0)

                batch_train_loss.append(loss.item())
        

            total = correct = 0
            all_val_labels = []
            all_val_preds = []
            all_val_probs = []

            self.model.eval()
            with torch.no_grad():
                for val_batch in valid_dataloader:
                    val_labels = val_batch["label"].to(self.device)
                    val_input_ids = val_batch["input_ids"].to(self.device)
                    val_attention_mask = val_batch["attention_mask"].to(self.device)
                    
                    val_outputs = self.model(val_input_ids, attention_mask=val_attention_mask)
                    val_logits = val_outputs.logits
                    val_loss = criterion(val_logits, val_labels)
                    val_preds = val_logits.argmax(dim=1)

                    # Store for metrics calculation
                    all_val_labels.extend(val_labels.cpu().numpy())
                    all_val_preds.extend(val_preds.cpu().numpy())
                    all_val_probs.extend(torch.softmax(val_logits, dim=1).cpu().numpy())

                    correct += (val_preds == val_labels).sum().item()
                    total += val_labels.size(0)

                    batch_valid_loss.append(val_loss.item())

            train_accuracy = correct / total
            valid_accuracy = correct / total
            train_loss = sum(batch_train_loss) / len(train_dataloader)
            valid_loss = sum(batch_valid_loss) / len(train_dataloader)

            self.log_accuracy_loss(train_accuracy, train_loss, valid_accuracy, valid_loss, epoch)
            f1 = self.log_pr_curve_and_f1(all_val_labels, all_val_preds, all_val_probs, epoch)
            self.log_dataset_text(dataset, max_texts=30)


        self.writer.add_hparams({
            "learning_rate": float(self.params["learning_rate"]),
            "batch_size": self.params["batch_size"],
            "epochs": self.params["epochs"],
            "test_size": self.params["test_size"],
            "random_state": self.params["random_state"]
            },
            {
                'Accuracy/train': train_accuracy,
                'Accuracy/test': valid_accuracy,
                'F1 Score/test': f1
            }
        )

        self.writer.close()

        torch.save(self.model.state_dict(), self.params["save_state_dict_dir"]+f"{NOW_TIME}.pt")


    def log_accuracy_loss(self, train_accuracy, train_loss, valid_accuracy, valid_loss, epoch):
        # Log validation accuracy and loss
        self.writer.add_scalar("Accuracy/train", train_accuracy, epoch)
        self.writer.add_scalar("Loss/train", train_loss, epoch)
        self.writer.add_scalar("Accuracy/test", valid_accuracy, epoch)
        self.writer.add_scalar("Loss/test", valid_loss, epoch)


    def log_pr_curve_and_f1(self, all_val_labels, all_val_preds, all_val_probs, epoch):
        """
        Log PR curve and F1 score for multiclass classification.
        """
        import numpy as np
        from sklearn.metrics import precision_recall_curve, f1_score
        
        # Calculate F1 score for macro average (across all classes)
        f1 = f1_score(all_val_labels, all_val_preds, average='macro')
        self.writer.add_scalar("F1 Score/test", f1, epoch)

        # Loop over each class and calculate PR curve
        for class_idx in range(len(set(all_val_labels))):  # Assuming all_val_labels is a list of true labels
            class_labels = np.array([1 if label == class_idx else 0 for label in all_val_labels])
            class_probs = np.array([prob[class_idx] for prob in all_val_probs])

            precision, recall, _ = precision_recall_curve(class_labels, class_probs)
            
            # Convert precision and recall to numpy arrays for TensorBoard
            self.writer.add_pr_curve(f'PR Curve/Class {class_idx}', class_labels, class_probs, epoch)

        return f1


    def log_dataset_text(self, dataset, max_texts=30):
        for i in range(max_texts):
            text = dataset.getText(i)
            self.writer.add_text(f'Dataset Text {i}', text, global_step=0)