class HandWrittenCNN(nn.Module):
  def __init__(self):
    super(HandWrittenCNN, self).__init__()

    """
    Building blocks of convolutional neural network.

    Parameters:
      * in_channels: Number of channels in input image
      * num_classes: Number of classes in output, i.e. number of digits
    """

    self.conv1 = nn.Conv2d(in_channels=1, out_channels=8, kernel_size=3, stride=1, padding=1)
    self.conv2 = nn.Conv2d(in_channels=8, out_channels=16, kernel_size=3, stride=1, padding=1)
    self.pool = nn.MaxPool2d(kernel_size=2, stride=2)
    self.fc = nn.Linear(in_features=16*7*7, out_features=10)  # Changed from 9 to 10 for digits 0-9

  def forward(self, x):
    x = F.relu(self.conv1(x))
    x = self.pool(x)
    x = F.relu(self.conv2(x))
    x = self.pool(x)
    x = x.reshape(x.shape[0], -1)
    x = self.fc(x)
    return x 