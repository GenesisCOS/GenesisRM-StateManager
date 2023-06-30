import torch
import torch.nn as nn
import numpy as np


class Predictor(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, output_size, device='cpu'):
        super(Predictor, self).__init__()

        self.input_size = input_size
        self.hidden_size = hidden_size
        self.output_size = output_size
        self.num_layers = num_layers

        self.device = torch.device(device)
        print(f'LSTM Predictor use device {device}')

        self.lstm1 = nn.LSTM(self.input_size, self.hidden_size, self.num_layers, batch_first=True)
        self.output_linear = nn.Linear(self.hidden_size, self.output_size)

    def forward(self, x: torch.Tensor):
        pred, _ = self.lstm1(x)
        pred = self.output_linear(pred)
        return pred[:, -1, :]

    def predict(self, x: np.ndarray) -> np.ndarray:
        pred = self(torch.tensor(x, dtype=torch.float32, device=self.device))
        return pred.detach().numpy()

    def load_model(self):
        pass
