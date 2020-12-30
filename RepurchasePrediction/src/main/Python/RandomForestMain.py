import random
import numpy as np
from sklearn.tree import DecisionTreeClassifier
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.model_selection import KFold, train_test_split
from sklearn.metrics import roc_curve, auc


class MyRandomForestCLF:
    def __init__(self, n_estimators, max_features):
        self.n_estimators = n_estimators
        self.max_features = max_features
        self.treeCLFs = []     # 基决策树列表
        self.treeFeatures = [] # 每个决策树用到的特征列

    def fit(self, train_X, train_y):
        num_features = train_X.shape[1] # 样本的特征个数
        index_feature = [i for i in range(num_features)]
        index_samples = [i for i in range(train_X.shape[0])]
        # 构建决策树
        for i in range(self.n_estimators):
            # 1.随机选取一些特征列
            used_feature = random.sample(index_feature, random.randint(5, self.max_features))
            self.treeFeatures.append(used_feature)
            # 2.随机选取一些样本
            used_sample = random.sample(index_samples, random.randint(1, train_X.shape[0]))
            temp = train_X[used_sample,:]
            used_train_X = temp[:, used_feature]
            used_train_y = train_y[used_sample]
            # 3.构造基决策树
            clf = DecisionTreeClassifier()
            clf.fit(used_train_X, used_train_y)
            self.treeCLFs.append(clf)

    def predict(self, test_X):
        result = np.zeros((1, test_X.shape[0]))
        i = 0
        for clf in self.treeCLFs:
            predict_y = clf.predict(test_X[:, self.treeFeatures[i]])
            result += predict_y
            i += 1
        result = np.sign(result)
        # 对计算出来为0的数据进行处理
        for j in range(result.shape[1]):
            if result[0][j] == 0:
                result[0][j] = random.choice([1,-1])
        return (result + 1) / 2

    def predict_proba(self, test_X):
        result = np.zeros((test_X.shape[0], 2))
        i = 0
        for clf in self.treeCLFs:
            predict_y = clf.predict_proba(test_X[:, self.treeFeatures[i]])
            result += predict_y
            i += 1
        result = result / len(self.treeCLFs)
        return result

def get_data() :
    '''
    获取数据，并进行数据预处理
    :return: 数据特征和数据标签
    '''
    # 读入数据
    trainDf = pd.read_csv("../../../processed_data/train.csv")
    trainDf = trainDf.drop('user_id', axis=1)
    trainDf = trainDf.drop('merchant_id', axis=1)
    
    testDf  = pd.read_csv("../../../processed_data/test.csv")
    testDf = testDf.drop('user_id', axis=1)
    testDf = testDf.drop('merchant_id', axis=1)
    
    cols = list(trainDf.columns)  # 特征数据的列
    cols.remove('label')
    train_X = trainDf[cols].values  # 训练特征
    train_y = trainDf['label'].values  # 训练标签
    test_X = testDf[cols].values
    return train_X, train_y, test_X

def main():
    X, y, tX = get_data()

    # 选择集成分类器
    clf = MyRandomForestCLF(500, 10)

    ######## 普通的train_test_split ######
    train_X, test_X, train_y, test_y = train_test_split(X, y)
    clf.fit(train_X, train_y)
    result = clf.predict(test_X)
    #####################################

    # ############ 五折交叉验证 ############
    # kf = KFold(n_splits=5)
    # tprs = [] # true positive rates
    # aucs = []
    # xvals = np.linspace(0, 1, 100)
    # i = 0
    # for train_index, test_index in kf.split(X):
    #     # 获取到相应的训练与测试样本
    #     train_X, train_y = X[train_index], y[train_index]
    #     test_X, test_y = X[test_index], y[test_index]
    #     clf.fit(train_X, train_y)
    #     probas_ = clf.predict_proba(test_X)
    #     # Compute ROC curve and area of the curve
    #     fpr, tpr, thresholds = roc_curve(test_y, probas_[:, 1])
    #     yinterp = np.interp(xvals, fpr, tpr)
    #     tprs.append(yinterp)
    #     tprs[-1][0] = 0
    #     roc_auc = auc(fpr, tpr)
    #     aucs.append(roc_auc)
    #     # plt.plot(fpr, tpr, lw=1,
    #     #          label='ROC fold %d (AUC = %0.2f)' % (i, roc_auc))
    #     i += 1
    # # 画ROC的平均值曲线
    # mean_fpr = np.linspace(0, 1, 100)
    # mean_tpr = np.mean(tprs, axis=0)
    # mean_tpr[-1] = 1.0
    # mean_auc = auc(mean_fpr, mean_tpr)
    # print('5折交叉验证AUC：', mean_auc)
    # std_auc = np.std(aucs)
    # # plt.plot(mean_fpr, mean_tpr, color='b',
    # #          label=r'Mean ROC (AUC = %0.2f $\pm$ %0.2f)' % (mean_auc, std_auc),
    # #          lw=2, alpha=.8)
    # std_tpr = np.std(tprs, axis=0)
    # tprs_upper = np.minimum(mean_tpr + std_tpr, 1)
    # tprs_lower = np.maximum(mean_tpr - std_tpr, 0)
    # # plt.fill_between(mean_fpr, tprs_lower, tprs_upper, color='grey', alpha=.2,
    # #                  label=r'$\pm$ 1 std. dev.')
    # # plt.plot([0, 1], [0, 1], linestyle='--', lw=2, color='r',
    # #          label='Chance', alpha=.8)
    # # plt.legend(loc="lower right")
    # # plt.show()
    # ############ 五折交叉验证 ############
        
    ############ 测试集上运用 ############
    result = clf.predict(tX)
    prob = clf.predict_proba(tX)
    pd.DataFrame(prob).to_csv('../../../output/RandomForest/sample_500.csv',index=False)
    ############ 测试集上运用 ############
if __name__ == '__main__':
    main()