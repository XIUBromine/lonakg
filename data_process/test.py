import tkinter as tk
from tkinter import ttk


class App:
    def __init__(self, root):
        self.root = root
        self.root.title("数据导入界面")
        self.root.geometry("900x500")

        # 主布局：左右分栏
        main_frame = tk.Frame(root)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # ================= 左侧菜单 =================
        left_frame = tk.Frame(main_frame, width=150, bd=1, relief="solid")
        left_frame.pack(side="left", fill="y", padx=(0, 10))
        left_frame.pack_propagate(False)

        tk.Label(left_frame, text="数据导入", font=("Arial", 12)).pack(pady=15)

        tk.Button(left_frame, text="导入日志", width=12).pack(pady=10)
        tk.Button(left_frame, text="接口测试", width=12).pack(pady=10)

        # ================= 右侧内容 =================
        right_frame = tk.Frame(main_frame, bd=1, relief="solid")
        right_frame.pack(side="right", fill="both", expand=True)

        # ====== 顶部 Tabs ======
        notebook = ttk.Notebook(right_frame)
        notebook.pack(fill="both", expand=True, padx=5, pady=5)

        tab1 = tk.Frame(notebook)
        tab2 = tk.Frame(notebook)
        tab3 = tk.Frame(notebook)

        notebook.add(tab1, text="*客户信息log")
        notebook.add(tab2, text="*订单order信息")
        notebook.add(tab3, text="黑名单log")

        # ================= tab1 内容 =================
        content_frame = tk.Frame(tab1)
        content_frame.pack(fill="both", expand=True)

        # 中间虚线框（用Canvas模拟）
        canvas = tk.Canvas(content_frame, height=200)
        canvas.pack(fill="x", padx=50, pady=40)

        # 画虚线矩形
        canvas.create_rectangle(
            10, 10, 600, 180,
            dash=(5, 3)
        )

        # 中间按钮（上传）
        upload_btn = tk.Button(content_frame, text="上传当日增量数据表", width=20)
        canvas.create_window(310, 80, window=upload_btn)

        # 提示文字
        canvas.create_text(310, 120, text="支持csv格式", fill="gray")

        # 底部“开始导入”按钮
        start_btn = tk.Button(content_frame, text="开始导入", width=12)
        start_btn.pack(anchor="e", padx=30, pady=10)


if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()