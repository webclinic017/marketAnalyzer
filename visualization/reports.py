#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 26 11:22:39 2022

@author: manuel
"""
from fpdf import FPDF
class PDF(FPDF):
    def __init__(self):
        super().__init__()
        self.WIDTH = 400
        self.HEIGHT = 500
      
   
        
    def footer(self):
        # Page numbers in the footer
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.set_text_color(128)
        self.cell(0, 10, 'Page ' + str(self.page_no()), 0, 0, 'C')
      
     
    def page_text(self):
         
        self.set_font('Arial', 'B', 10)
        self.cell(60, 1, self.title, 0, 1, 'C')
        self.ln(1)
        # Determine how many plots there are per page and set positions
        # and margins accordingly
        self.set_font('Arial','', 8)
        self.multi_cell( w=0, h=10, txt=self.text,border=1, align='J')
    def page_body(self):
        # Determine how many plots there are per page and set positions
        # and margins accordingly
        print(self.image1)
        self.image(self.image1,15,20,200)
            
            
    def print_page(self,text,image,image2,title):
        # Generates the report
        self.text=text
        self.image1=image
        self.title=title
        self.add_page()
        self.page_text()
        self.add_page()
        self.page_body()
        self.image1=image2
        self.add_page()
        self.page_body()