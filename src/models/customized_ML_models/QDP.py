import numpy as np
import pandas as pd
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import QuantileRegressor
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt

class QuadraticUpperBoundQuantileRegressor:
    """
    مدل رگرسیون چارک با رابطه درجه دو برای یافتن پوش بیشینه
    """
    
    def __init__(self, quantile=0.95, alpha=0.9, fit_intercept=True):
        self.quantile = quantile
        self.alpha = alpha
        self.fit_intercept = fit_intercept
        self.poly = PolynomialFeatures(degree=2, include_bias=fit_intercept)
        self.model = None
        
    def fit(self, X, y):
        X_poly = self.poly.fit_transform(X)
        print(X_poly[0])
        # آموزش مدل رگرسیون چارک
        self.model = QuantileRegressor(
            quantile=self.quantile,
            alpha=self.alpha,
            solver='highs'
        )
        self.model.fit(X_poly, y)
        
        # ذخیره ضرایب
        self.coef_ = self.model.coef_
        if self.fit_intercept:
            self.intercept_ = self.model.intercept_
        else:
            self.intercept_ = 0
            
        return self
    
    def predict(self, X):
        """
        پیش‌بینی پوش بیشینه
        """
        if self.model is None:
            raise ValueError("مدل باید ابتدا آموزش دیده باشد (fit)")
        
        X_poly = self.poly.transform(X)
        print(X)
        return self.model.predict(X_poly)
    
    def get_coefficients(self):
        """
        برگرداندن ضرایب مدل به صورت خوانا
        """
        if self.model is None:
            raise ValueError("مدل باید ابتدا آموزش دیده باشد")
        
        # نام‌گذاری ضرایب
        feature_names = self.poly.get_feature_names_out()
        
        coef_dict = {}
        for name, coef in zip(feature_names, self.coef_):
            if abs(coef) > 1e-10:  # فقط ضرایب غیرصفر
                coef_dict[name] = coef
                
        return coef_dict
    
    def evaluate(self, X_test, y_test):
        """
        ارزیابی مدل روی داده تست
        """
        y_pred = self.predict(X_test)
        
        # محاسبه معیارهای ارزیابی پوش بیشینه
        results = {}
        
        # 1. پوشش (Coverage)
        coverage = np.mean(y_test <= y_pred)
        results['coverage'] = coverage
        
        # 2. نسبت نقاطی که مدل کمتر از واقع پیش‌بینی کرده
        under_prediction_ratio = np.mean(y_test > y_pred)
        results['under_prediction_ratio'] = under_prediction_ratio
        
        # 3. میانگین خطا در موارد under-prediction
        exceedance_mask = y_test > y_pred
        if np.any(exceedance_mask):
            mean_exceedance = np.mean(y_test[exceedance_mask] - y_pred[exceedance_mask])
            results['mean_exceedance'] = mean_exceedance
        else:
            results['mean_exceedance'] = 0
            
        # 4. حجم منطقه اطمینان
        confidence_band_width = np.mean(y_pred - y_test)
        results['confidence_band_width'] = confidence_band_width
        
        # 5. Quantile Loss
        errors = y_test - y_pred
        quantile_loss = np.mean(np.maximum(self.quantile * errors, (self.quantile - 1) * errors))
        results['quantile_loss'] = quantile_loss
        
        return results
    
    def plot_results(self, X, y, title="پوش بیشینه درجه دو"):
        """
        رسم نتایج مدل
        """
        if X.shape[1] != 1:
            print("رسم فقط برای داده تک بعدی قابل نمایش است")
            return
            
        # مرتب کردن داده‌ها برای رسم
        idx = np.argsort(X.flatten())
        X_sorted = X[idx]
        y_sorted = y[idx]
        
        # پیش‌بینی برای نقاط مرتب شده
        y_pred = self.predict(X_sorted)
        
        # رسم
        plt.figure(figsize=(12, 6))
        
        # داده واقعی
        plt.scatter(X_sorted, y_sorted, alpha=0.6, label='داده واقعی', s=20)
        
        # پوش بیشینه
        plt.plot(X_sorted, y_pred, 'r-', linewidth=2, label=f'پوش بیشینه (چارک {self.quantile})')
        
        # پر کردن منطقه بالای منحنی
        plt.fill_between(X_sorted.flatten(), y_pred, y_pred.max() + 1, 
                        alpha=0.2, color='red', label='منطقه اطمینان')
        
        plt.xlabel('ورودی (X)')
        plt.ylabel('خروجی (y)')
        plt.title(title)
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()
        
        # محاسبه و نمایش معیارها
        results = self.evaluate(X, y)
        print("\n📊 معیارهای ارزیابی:")
        print(f"   پوشش (Coverage): {results['coverage']:.2%}")
        print(f"   نسبت نقاط بالاتر از پوش: {results['under_prediction_ratio']:.2%}")
        print(f"   میانگین خطا در این نقاط: {results['mean_exceedance']:.4f}")
        print(f"   Quantile Loss: {results['quantile_loss']:.4f}")


# مثال استفاده:
if __name__ == "__main__":
    # تولید داده نمونه با رابطه درجه دو + نویز
    np.random.seed(42)
    n_samples = 10
    
    # داده ورودی
    X = np.random.uniform(-5, 5, n_samples).reshape(-1, 1)
    
    # رابطه درجه دو واقعی
    y_true = 0.5 * X**2 + 2 * X + 1
    
    # اضافه کردن نویز ناهمسان وار (نویز بیشتر برای مقادیر بزرگ)
    noise = np.random.normal(0, 0.5 + 0.2 * np.abs(X), (n_samples, 1))
    y_observed = y_true + noise
    
    # آموزش مدل برای پوش بیشینه (چارک 95%)
    model = QuadraticUpperBoundQuantileRegressor(quantile=0.5)
    model.fit(X, y_observed.flatten())
    
    # نمایش ضرایب
    print("######## get_coefficients ########")#"ضرایب مدل درجه دو:")
    print(model.predict(X),model.intercept_)
    coefs = model.get_coefficients()
    for name, coef in coefs.items():
        print(f"  {name}: {coef:.4f}")
    
    # ارزیابی
    results = model.evaluate(X, y_observed.flatten())
    print("\n######## items ########")
    for key, value in results.items():
        print(f"  {key}: {value:.4f}")
    
    # رسم نتایج
    model.plot_results(X, y_observed.flatten(), 
                      title="پوش بیشینه درجه دو برای داده‌های نمونه")