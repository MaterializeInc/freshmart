export const mantineTheme = {
  colorScheme: 'dark',
  fontFamily: 'Inter, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
  fontFeatureSettings: '"tnum", "lnum", "cv06", "cv10"',
  fontSize: {
    xs: '12px',
    sm: '14px',
    md: '16px',
    lg: '18px',
    xl: '20px',
  },
  headings: {
    fontFamily: 'Inter, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
    fontFeatureSettings: '"tnum", "lnum", "cv06", "cv10"',
  },
  components: {
    Paper: {
      defaultProps: {
        shadow: 'sm',
        radius: 'sm',
        withBorder: true,
      },
      styles: (theme) => ({
        root: {
          backgroundColor: 'rgb(13, 17, 22)',
          borderColor: theme.colors.dark[5],
          transition: 'background-color 0.2s ease',
        },
      }),
    },
    Button: {
      defaultProps: {
        radius: 'sm',
      },
      styles: () => ({
        root: {
          fontFamily: 'Inter, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
          fontFeatureSettings: '"tnum", "lnum", "cv06", "cv10"',
          transition: 'all 0.2s ease',
        },
      }),
    },
    Container: {
      defaultProps: {
        size: 'xl',
      },
      styles: {
        root: {
          maxWidth: '1400px',
        },
      },
    },
    Text: {
      styles: {
        root: {
          fontFeatureSettings: '"tnum", "lnum", "cv06", "cv10"',
        },
      },
    },
    Badge: {
      styles: () => ({
        root: {
          fontFamily: 'Inter, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
          fontFeatureSettings: '"tnum", "lnum", "cv06", "cv10"',
        },
      }),
    },
    Accordion: {
      styles: () => ({
        item: {
          backgroundColor: 'transparent',
          border: 'none',
        },
        control: {
          backgroundColor: 'transparent',
          '&:hover': {
            backgroundColor: 'rgba(255, 255, 255, 0.05)',
          },
          '&[data-active="true"]': {
            backgroundColor: 'transparent',
          },
        },
        content: {
          backgroundColor: 'transparent',
        },
        chevron: {
          color: '#BCB9C0',
        },
      }),
    },
    Select: {
      styles: {
        dropdown: {
          backgroundColor: 'rgb(13, 17, 22) !important',
          borderColor: 'rgba(255, 255, 255, 0.1) !important',
        },
        item: {
          backgroundColor: 'rgb(13, 17, 22) !important',
          color: '#BCB9C0 !important',
          '&[data-selected]': {
            backgroundColor: 'rgba(255, 255, 255, 0.1) !important',
            color: '#BCB9C0 !important',
          },
          '&[data-hovered]': {
            backgroundColor: 'rgba(255, 255, 255, 0.05) !important',
            color: '#BCB9C0 !important',
          },
        },
      },
    },
  },
  colors: {
    dark: [
      '#F8F9FA',
      '#E9ECEF',
      '#DEE2E6',
      '#CED4DA',
      '#BCB9C0',
      '#66626A',
      '#323135',
      '#212529',
      '#0D1116',
      '#0D1116',
    ],
  },
  other: {
    transition: {
      default: '0.2s ease',
    },
  },
};

export const globalStyles = {
  body: {
    backgroundColor: 'rgb(13, 17, 22) !important',
    color: '#BCB9C0',
    fontFamily: 'Inter, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
    fontFeatureSettings: '"tnum", "lnum", "cv06", "cv10"',
    fontSize: '14px',
    lineHeight: 1.5,
    margin: 0,
    padding: 0,
  },
  '#root': {
    backgroundColor: 'rgb(13, 17, 22)',
    minHeight: '100vh',
  },
  '.mantine-Container-root': {
    backgroundColor: 'rgb(13, 17, 22)',
  },
  pre: {
    fontFamily: 'SFMono-Regular, Menlo, Monaco, Consolas, monospace',
    whiteSpace: 'pre-wrap',
    overflowWrap: 'break-word',
    backgroundColor: 'rgba(255, 255, 255, 0.05)',
    padding: '1rem',
    borderRadius: '4px',
    border: '1px solid rgba(255, 255, 255, 0.1)',
    color: '#BCB9C0',
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    'th, td': {
      padding: '8px',
      borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
      fontFeatureSettings: '"tnum", "lnum", "cv06", "cv10"',
      color: '#BCB9C0',
    },
    th: {
      textAlign: 'left',
      fontWeight: 600,
      color: '#BCB9C0',
    },
  },
};

export const chartTheme = {
  background: 'rgb(13, 17, 22)',
  textColor: '#BCB9C0',
  fontSize: 12,
  axis: {
    domain: {
      line: {
        stroke: '#66626A',
        strokeWidth: 1,
      },
    },
    ticks: {
      line: {
        stroke: '#66626A',
        strokeWidth: 1,
      },
    },
  },
  grid: {
    line: {
      stroke: '#323135',
      strokeWidth: 1,
    },
  },
};
