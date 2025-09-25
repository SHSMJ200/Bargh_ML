import sys
import os
# تنظیم متغیر محیطی برای غیرفعال کردن هشدار oneDNN در TensorFlow
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

# اضافه کردن مسیر پروژه به sys.path جهت وارد کردن ماژول‌ها
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

from tensorflow.keras.layers import Dense, Input,Concatenate
from tensorflow.keras.models import Model

from main import *
from models import Neural_network

# تنظیمات نمایش داده‌های pandas
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 100)

# راهنما: انتخاب ویژگی‌ها و دریافت X و y از دیتافریم
def select_features_and_get_X_and_y(df, features_to_be_select, is_mimo=False, number_mimo=None):
    """
    انتخاب ویژگی‌ها و استخراج ماتریس ورودی X و بردار هدف y از دیتافریم

    پارامترها:
    -----------
    df : pandas.DataFrame
        دیتافریمی که داده‌ها در آن هستند.
    features_to_be_select : list of str
        لیست نام ویژگی‌هایی که باید انتخاب شوند.
    is_mimo : bool, اختیاری
        مشخص می‌کند آیا مدل چند ورودی چند خروجی (MIMO) است یا خیر.
    number_mimo : int, اختیاری
        تعداد خروجی‌های MIMO در صورت فعال بودن.

    خروجی:
    -------
    X : numpy.ndarray
        ماتریس ویژگی‌ها
    y : numpy.ndarray
        بردار یا ماتریس هدف
    """
    feature_selector = Feature_selector(df, target="generation")
    # اضافه کردن ویژگی با تاخیر 24 ساعته به لیست ویژگی‌ها
    features_to_be_select.append(f"generation_with_{24}_delay")
    feature_selector.select(features_to_select=features_to_be_select)
    X, y = feature_selector.get_X_and_y(is_mimo=is_mimo, number_mimo=number_mimo)
    return X, y

# ساخت مدل شبکه عصبی 4 لایه با تابع فعال‌سازی ReLU
'''
model = Sequential()
model.add(Dense(64, activation='relu', input_shape=(X.shape[1],)))
model.add(Dense(64, activation='relu'))
model.add(Dense(32, activation='relu'))
model.add(Dense(16, activation='relu'))
model.add(Dense(1, activation='linear'))  # لایه خروجی برای رگرسیون

# کامپایل مدل با Adam و خطای میانگین مربعات
model.compile(optimizer='adam', loss='mean_squared_error')
'''
#model.save(get_root() + '/models/model.model')
# آموزش مدل
# model.fit(X_train, y_train, epochs=100 , batch_size=32)
# ذخیره مدل در مسیر دلخواه
#model.save(get_root() + '/models/model.model')
#from tensorflow.keras.models import load_model
#model = load_model("my_model")


flag = 0

if flag == 0:#__name__ == "__main__":
    # TODO: for mimo > 1 doesn't work
    write_predictions = False

    number_mimo = 1
    is_mimo = number_mimo > 1
    y_is_flat = not is_mimo

    l_min = 4
    max_diff = 3
    c_thresh = 0.9

    df = add_features_and_filter(l_min, max_diff, c_thresh, read_from_integrated=False)
    logger.info(f"Csv file has bean labeled successfully")

    
    
    # تعریف ویژگی‌های انتخابی اولیه
    features_to_be_select = [
        "name", "code", "temperature", "humidity", "dew", "surface_pressure", "value", "forecast",
        "status", "season", "datetime"
    ]

    # افزودن ویژگی‌های تاخیر برای تعدادی از ویژگی‌ها
    space_features = ["temperature", "humidity", "dew", "surface_pressure", "value", "forecast", "status"]
    fa = Feature_adder(df, add_label_column=False)
    for feature in space_features:
        for i in range(3):
            fa.create_feature_with_delay(feature, i + 1)
            features_to_be_select.append(f"{feature}_with_{i + 1}_delay")


    ds = Data_selector(df)
    df_modified = ds.select_peaks(goodness=3)
    logger.info(f"Rows have been selected successfully")

    X, y = select_features_and_get_X_and_y(df_modified, features_to_be_select, is_mimo=is_mimo, number_mimo=number_mimo)
    logger.info(f"Some features have been dropped successfully")

    # model = Random_Forest(n_estimators=100, max_depth=1000)
    # model = Linear()
    # model = Polynomial(degree=2)
    # model = XGBoost(n_estimators=1000, max_depth=5)
    # model = Neural_network(input_dim=X.shape[1], epochs=100, verbose=1)

    inputs = Input(shape=(X.shape[1],))
    layer1 = Dense(32, activation='relu')(inputs)
    
    path2 = Dense(8, activation='tanh')(layer1)
    path3 = Dense(units=10, activation='softmax')(layer1)
    merged1 = Concatenate()([layer1, path2, path3])
    layer1 = Dense(8, activation='relu')(merged1)
    
    path2 = Dense(8, activation='tanh')(layer1)
    path3 = Dense(units=10, activation='softmax')(layer1)
    merged1 = Concatenate()([layer1, path2, path3])
    layer1 = Dense(8, activation='relu')(merged1)
    
    output = Dense(1, activation='linear')(layer1)
    
    model_m = Model(inputs=inputs, outputs=output)
    model_m.compile(optimizer='adam', loss='mean_squared_error')
    
    model = Neural_network(input_dim=X.shape[1], epochs=100, verbose=1)
    model.model = model_m

    model.scale_and_split_data(X, y, y_is_flat=y_is_flat)
    model.fit()
    logger.info(f"Model has been trained successfully")

    test_model(model)

    if write_predictions:
        write_result(df, model, X)

if flag == 1:

    # لایه ورودی با 64 ورودی
    inputs = Input(shape=(64,))
    layer1 = Dense(32, activation='relu')(inputs)
    path1 = Dense(16, activation='relu')(layer1)
    path2 = Dense(16, activation='tanh')(layer1)
    merged = Concatenate()([path1, path2])
    layer3 = Dense(8, activation='relu')(merged)
    output = Dense(1, activation='linear')(layer3)
    model = Model(inputs=inputs, outputs=output)

    # نمایش خلاصه مدل
    print(model.summary())