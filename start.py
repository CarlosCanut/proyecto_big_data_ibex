#!/usr/bin/env python3

import npyscreen
import curses

class App(npyscreen.StandardApp):
    def onStart(self):
        self.addForm("MAIN", MainForm, name="Hello Medium!")

# caja de arriba
class InputBox1(npyscreen.BoxTitle):
    _contained_widget = npyscreen.MultilineEdit
    self.add(npyscreen.TitleMultiSelect, relx=x // 2 + 1, rely=2, value=[1, 2], name="Pick Several", values=["Option1", "Option2", "Option3"], scroll_exit=True)

class MainForm(npyscreen.FormBaseNew):
    # Constructor
    def create(self):
        # Add The TitleText widget to the form
        y, x = self.useable_space()
        self.title = self.add(npyscreen.TitleText, name="TitleText", value="Hello World!")
        self.InputBox1 = self.add(InputBox1, name="New Windows", max_height=y // 2)

    def on_ok(self):
        self.parentApp.setNextForm(None)

    def on_cancel(self):
        self.title.value = "Hello World!"

MyApp = App()

MyApp.run()


