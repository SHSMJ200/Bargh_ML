import sys, os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
from src.models.train_model.train_model import *
pd.set_option("display.max_columns",None)
pd.set_option("display.max_rows",100)

logger = CustomLogger(name="model_main").get_logger()
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from sklearn.metrics import mean_absolute_error, mean_squared_error
import warnings
warnings.filterwarnings('ignore')
import numpy as np
from tensorflow.keras.layers import Input, GRU, Dense, Concatenate, Dropout, Flatten, BatchNormalization
from tensorflow.keras.models import Model
from tensorflow.keras.optimizers import Adam

# پارامترها
timesteps = 120
features_production = 1
features_env = 2
future_features_env = 2

# ورودی‌ها
input_production = Input(shape=(timesteps, features_production), name='production_input')
input_env = Input(shape=(timesteps, features_env), name='env_input')
input_future_env = Input(shape=(24, future_features_env), name='future_env_input')

# نرمال سازی داده‌های ورودی تاریخی
norm_production = BatchNormalization()(input_production)
norm_env = BatchNormalization()(input_env)

# ادغام ورودی‌های تاریخی
concat_hist = Concatenate(axis=-1)([norm_production, norm_env])

# لایه‌های چندمرحله‌ای GRU با Dropout
gru_out = GRU(128, return_sequences=True)(concat_hist)
gru_out = Dropout(0.3)(gru_out)
gru_out = GRU(64, return_sequences=False)(gru_out)
gru_out = Dropout(0.3)(gru_out)

# فلت کردن داده‌های آینده (دمای پیش‌بینی شده و فشار فردا)
flat_future_env = Flatten()(input_future_env)

# ادغام خروجی GRU با داده‌های آینده
concat_all = Concatenate(axis=-1)([gru_out, flat_future_env])

# لایه Dense برای پیش‌بینی 24 ساعت آینده
output = Dense(24, activation='linear')(concat_all)

# مدل نهایی
model = Model(inputs=[input_production, input_env, input_future_env], outputs=output)

# کامپایل مدل
model.compile(optimizer=Adam(learning_rate=0.001), loss='mse')

# آموزش اولیه: فرض بر این است که داده‌های زیر آماده شده‌اند
# X_production_train: شکل (نمونه‌ها, 120, 1)
# X_env_train: شکل (نمونه‌ها, 120, 2)
# X_future_env_train: شکل (نمونه‌ها, 24, 2) - شامل دما و فشار پیش‌بینی شده فردا
# y_train: شکل (نمونه‌ها, 24) - تولید نیروگاه برای 24 ساعت آینده

# model.fit([X_production_train, X_env_train, X_future_env_train], y_train, epochs=100, batch_size=32)

# تابع پیش بینی و بروزرسانی برخط مدل
def online_predict_and_update(model, input_production_seq, input_env_seq, input_future_env_seq, true_values_24h):
    """
    input_production_seq: آرایه (1, 120, 1)
    input_env_seq: آرایه (1, 120, 2)
    input_future_env_seq: آرایه (1, 24, 2)
    true_values_24h: آرایه (1, 24)
    """
    pred = model.predict([input_production_seq, input_env_seq, input_future_env_seq])
    
    # بروزرسانی داده‌های ورودی با مقدار واقعی
    updated_production = np.append(input_production_seq[:, 24:, :], true_values_24h.reshape(1, 24, 1), axis=1)
    updated_env = input_env_seq[:, 24:, :]  # فرض شده داده جدید محیطی بعدا بروز شود
    updated_future_env = input_future_env_seq  # داده آینده ثابت باقی می‌ماند تا فردا
    
    # یک گام آموزش سریع با داده جدید
    model.fit([updated_production, updated_env, updated_future_env], true_values_24h, epochs=1, verbose=0)
    
    return pred, updated_production, updated_env, updated_future_env


csv_semi_processed_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")
df = pd.read_csv(csv_semi_processed_path, encoding='utf-8')

add_is_test_column(df)
save_model = True
save_model_folder = os.path.join(project_root, "src", "models", "fitted_models")
write_predictions = False
n_mimo = 4

train_test_ds = Data_selector(Data_selector(df).select_peaks(goodness=3))
train_df = train_test_ds.select_train_test(is_test=False)
test_df = train_test_ds.select_train_test(is_test=True)


base_features = ["name", "code", "temperature", "humidity", "dew", "surface_pressure", "value",
                     "forecast", "status", "season", "datetime", "generation_with_24_delay"]

base_feature_selector = Feature_selector(train_df, target="generation")
base_feature_selector.select(features_to_select=base_features)
df_train = base_feature_selector.df.copy()

base_feature_selector = Feature_selector(test_df, target="generation")
base_feature_selector.select(features_to_select=base_features)
df_test = base_feature_selector.df.copy()

all_col = get_onehot_columns(df1=df_train,df2=df_test,n_mimo=n_mimo)